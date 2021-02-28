using System;
using System.Collections.Generic;
using System.Linq;
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
    public class DocumentsWriter : IDocumentsWriter
    {
        private readonly IDestinationRepositoryFactory _repositoryFactory;
        private readonly ChannelReader<List<ReplaceOneModel<BsonDocument>>> _batchReader;
        private readonly IProgressNotifier _notifier;
        private readonly bool _upsert;
        private readonly bool _dryRun;
        private readonly ILogger _logger;
        private readonly Channel<ReplaceOneModel<BsonDocument>> _retriesChannel;
        private readonly Task<(long processed, long retried, long failed)>[] _insertionWorkers;
        private readonly Task<(long processed, long retried, long failed)>[] _retryWorkers;
        private readonly string _collectionName;

        private const string ErrorUpdateWithMoveToAnotherShard =
            "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1";

        public DocumentsWriter(int insertionWorkersCount,
            int retryWorkersCount,
            string collectionName,
            IDestinationRepositoryFactory repositoryFactory,
            ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader,
            IProgressNotifier notifier,
            bool upsert,
            bool dryRun,
            ILogger logger)
        {
            _collectionName = collectionName;
            _repositoryFactory = repositoryFactory;
            _batchReader = batchReader;
            _notifier = notifier;
            _upsert = upsert;
            _dryRun = dryRun;
            _logger = logger;
            _insertionWorkers = new Task<(long processed, long retried, long failed)>[insertionWorkersCount];
            _retryWorkers = new Task<(long processed, long retried, long failed)>[retryWorkersCount];
            
            _retriesChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
        }

        public async Task<TransferResults> WriteAsync(CancellationToken token)
        {
            StartWorkers(token);
            return await WaitUntilFinishedAsync();
        }

        private void StartWorkers(CancellationToken token)
        {
            for (var workerN = 0; workerN < _insertionWorkers.Length; workerN++)
            {
                var workerLogger = _logger.ForContext("Scope", $"{_collectionName}-{workerN:00}");
                _insertionWorkers[workerN] = RunWorker(_repositoryFactory.Create(workerLogger), _retriesChannel.Writer,
                    workerLogger, token);
            }

            for (var retryWorkerN = 0; retryWorkerN < _retryWorkers.Length; retryWorkerN++)
            {
                var retryLogger = _logger.ForContext("Scope", $"{_collectionName}-Retry{retryWorkerN:00}");
                _retryWorkers[retryWorkerN] = RunRetryWorker(_repositoryFactory.Create(retryLogger), _retriesChannel.Reader,
                    retryLogger, token);
            }

            _logger.Debug("Started {I:N0} insertion worker(s) and {R:0} retry worker(s)", _insertionWorkers.Length, _retryWorkers.Length);
        }

        private async Task<TransferResults> WaitUntilFinishedAsync()
        {
            _logger.Debug("Waiting for insertion-workers to finish");
            await Task.WhenAll(_insertionWorkers);
            
            _logger.Debug("Insertion-workers finished, waiting for retry-workers");
            _retriesChannel.Writer.Complete();
            await Task.WhenAll(_retryWorkers);

            return await GetResultsAsync();
        }
        
        private async Task<(long processed, long totalRetried, long failed)> RunWorker(IDestinationRepository repository,
            ChannelWriter<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token)
        {
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

        private async Task<(long processed, long totalRetried, long failed)> RunRetryWorker(IDestinationRepository repository,
            ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token)
        {
            var totalProcessed = 0L;
            var totalFailed = 0L;
            
            await foreach (var failedReplace in failedWrites.ReadAllAsync(token))
            {
                var documentId = failedReplace.Replacement["_id"];
                try
                {
                    if (!_dryRun)
                    {
                        await repository.RetryReplaceAsync(failedReplace.Filter, failedReplace.Replacement, token);

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

        private async Task<TransferResults> GetResultsAsync()
        {
            var processed = 0L;
            var retried = 0L;
            var failed = 0L;

            foreach (var worker in _insertionWorkers.Concat(_retryWorkers))
            {
                var (s, r, f) = await worker;
                processed += s;
                retried += r;
                failed += f;
            }

            return new TransferResults(processed, retried, failed);
        }
    }
}