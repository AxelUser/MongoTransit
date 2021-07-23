using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Notifications;
using MongoTransit.Notifications.Notifiers;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Destination.Exceptions;
using Serilog;

namespace MongoTransit.Processing.Workers
{
    public class WriteWorkerFactory : IWriteWorkerFactory
    {
        public const string ErrorUpdateWithMoveToAnotherShard =
            "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1";

        private readonly ChannelReader<List<ReplaceOneModel<BsonDocument>>> _batchReader;
        private readonly IDestinationRepositoryFactory _repositoryFactory;
        private readonly IProgressNotifier _notifier;
        private readonly bool _dryRun;
        private readonly string _collectionName;

        public WriteWorkerFactory(ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader,
            IDestinationRepositoryFactory repositoryFactory,
            IProgressNotifier notifier,
            bool dryRun,
            string collectionName)
        {
            _batchReader = batchReader;
            _repositoryFactory = repositoryFactory;
            _notifier = notifier;
            _dryRun = dryRun;
            _collectionName = collectionName;
        }
        
        public async Task<WorkerResult> RunWorker(ChannelWriter<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token)
        {
            var repository = _repositoryFactory.Create(workerLogger);
            var totalSuccessful = 0L;
            var totalRetryable = 0L;
            var totalFailed = 0L;
            
            await foreach (var batch in _batchReader.ReadAllAsync(token))
            {
                try
                {
                    if (!_dryRun)
                    {
                        await repository.ReplaceManyAsync(batch, token);
                        totalSuccessful += batch.Count;
                    }

                    _notifier.Notify(batch.Count);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (ReplaceManyException rme) when(rme.Errors.Any())
                {
                    // TODO: notify about processed documents besides occurred errors
                    var retryable = 0;
                    var fails = 0;

                    var errSb = new StringBuilder();
                    foreach (var (index, message) in rme.Errors)
                    {
                        var failedRequest = batch[index];
                        errSb.AppendLine($"(ID: {failedRequest.Replacement["_id"]}) {message}");
                        switch (message)
                        {
                            case ErrorUpdateWithMoveToAnotherShard:
                                await failedWrites.WriteAsync(failedRequest, token);
                                retryable++;
                                break;
                            default:
                                fails++;
                                break;
                        }
                    }

                    totalFailed += fails;
                    totalRetryable += retryable;
                    totalSuccessful += rme.ProcessedCount - fails - retryable;

                    workerLogger.Error(
                        "{N:N0} documents failed to transfer, {R:N0} were sent to retry. Total batch: {B:N0}",
                        fails, retryable, batch.Count);
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
            
            return new WorkerResult(totalSuccessful, totalRetryable, totalFailed);

        }

        public async Task<WorkerResult> RunRetryWorker(ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
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
                        await repository.ReplaceDocumentAsync(failedReplace, token);

                        totalProcessed++;
                    }
                    _notifier.Notify(1);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    workerLogger.Error("Failed to retry replacement (ID: {Id}) in collection {Collection}: {Msg}",
                        documentId, _collectionName, e.Message);
                    workerLogger.Debug(e, "Retry exception details:");
                    totalFailed++;
                }
            }
            
            return new WorkerResult(totalProcessed, 0, totalFailed);

        }
    }
}