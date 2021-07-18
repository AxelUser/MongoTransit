using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Progress;
using MongoTransit.Storage.Destination;
using Serilog;

namespace MongoTransit.Workers
{
    public class WriteWorkerFactory : IWriteWorkerFactory
    {
        private const string ErrorUpdateWithMoveToAnotherShard =
            "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1";

        private readonly ChannelReader<List<ReplaceOneModel<BsonDocument>>> _batchReader;
        private readonly IDestinationRepositoryFactory _repositoryFactory;
        private readonly IProgressNotifier _notifier;
        private readonly bool _upsert;
        private readonly bool _dryRun;
        private readonly string _collectionName;

        public WriteWorkerFactory(ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader,
            IDestinationRepositoryFactory repositoryFactory,
            IProgressNotifier notifier,
            bool upsert,
            bool dryRun,
            string collectionName)
        {
            _batchReader = batchReader;
            _repositoryFactory = repositoryFactory;
            _notifier = notifier;
            _upsert = upsert;
            _dryRun = dryRun;
            _collectionName = collectionName;
        }
        
        public async Task<(long processed, long totalRetried, long failed)> RunWorker(
            ChannelWriter<ReplaceOneModel<BsonDocument>> failedWrites, ILogger workerLogger,
            CancellationToken token)
        {
            var repository = _repositoryFactory.Create(workerLogger);
            var totalProcessed = 0L;
            var totalRetried = 0L;
            var totalFailed = 0L;
            
            await foreach (var batch in _batchReader.ReadAllAsync(token))
            {
                try
                {
                    if (!_dryRun)
                    {
                        await repository.ReplaceManyAsync(batch, token);
                        totalProcessed += batch.Count;
                    }

                    _notifier.Notify(batch.Count);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (MongoBulkWriteException<BsonDocument> bwe)
                {
                    var retries = 0;
                    var fails = 0;

                    var errSb = new StringBuilder();
                    foreach (var error in bwe.WriteErrors)
                    {
                        var failedRequest = batch[error.Index];
                        errSb.AppendLine($"(ID: {failedRequest.Replacement["_id"]}) {error.Message}");
                        switch (error.Message)
                        {
                            case ErrorUpdateWithMoveToAnotherShard:
                                await failedWrites.WriteAsync(failedRequest, token);
                                retries++;
                                break;
                            default:
                                fails++;
                                break;
                        }
                    }

                    totalFailed += fails;
                    totalRetried += retries;
                    totalProcessed += bwe.Result.ProcessedRequests.Count - fails - retries;

                    workerLogger.Error(
                        "{N:N0} documents failed to transfer, {R:N0} were sent to retry. Total batch: {B:N0}",
                        fails, retries, batch.Count);
                    workerLogger.Debug("Bulk write exception details:\n{Errors}", errSb.ToString());
                }
                catch (Exception e)
                {
                    workerLogger.Error(e, "Error occurred while transferring documents");
                }
                finally
                {
                    token.ThrowIfCancellationRequested();
                }
            } 
            
            return (totalProcessed, totalRetried, totalFailed);

        }

        public async Task<(long processed, long totalRetried, long failed)> RunRetryWorker(
            ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites, ILogger workerLogger,
            CancellationToken token)
        {
            var repository = _repositoryFactory.Create(workerLogger);
            var totalProcessed = 0L;
            var totalFailed = 0L;
            
            await foreach (var failedReplace in failedWrites.ReadAllAsync(token))
            {
                var documentId = failedReplace.Replacement["_id"];
                try
                {
                    if (!_dryRun)
                    {
                        await repository.ReplaceDocumentAsync(failedReplace.Filter, failedReplace.Replacement, token);

                        totalProcessed++;
                    }
                    _notifier.Notify(1);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (MongoWriteException we)
                {
                    workerLogger.Error("Failed to retry replacement (ID: {Id}): {Msg}", documentId, we.Message);
                    workerLogger.Debug(we, "Retry exception details:");
                    totalFailed++;
                }
                catch (Exception e)
                {
                    workerLogger.Error(e, "Error occurred while transferring documents to {Collection}", _collectionName);
                    totalFailed++;
                }
            }
            
            return (totalProcessed, 0, totalFailed);

        }
    }
}