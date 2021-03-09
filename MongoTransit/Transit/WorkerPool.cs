using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Progress;
using Serilog;

namespace MongoTransit.Transit
{
    public class WorkerPool
    {
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly ChannelReader<(int count, ReplaceOneModel<BsonDocument>[] batch)> _batchReader;
        private readonly ProgressNotifier _notifier;
        private readonly bool _upsert;
        private readonly bool _dryRun;
        private readonly ILogger _logger;
        private readonly Channel<ReplaceOneModel<BsonDocument>> _retriesChannel;
        private readonly Task<(long processed, long retried, long failed)>[] _insertionWorkers;
        private readonly Task<(long processed, long retried, long failed)>[] _retryWorkers;
        private readonly string _collectionName;

        private const string ErrorUpdateWithMoveToAnotherShard =
            "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1";

        public WorkerPool(int insertionWorkersCount,
            int retryWorkersCount,
            IMongoCollection<BsonDocument> collection,
            ChannelReader<(int count, ReplaceOneModel<BsonDocument>[] batch)> batchReader,
            ProgressNotifier notifier,
            bool upsert,
            bool dryRun,
            ILogger logger)
        {
            _collection = collection;
            _collectionName = _collection.CollectionNamespace.CollectionName;
            _batchReader = batchReader;
            _notifier = notifier;
            _upsert = upsert;
            _dryRun = dryRun;
            _logger = logger;
            _insertionWorkers = new Task<(long processed, long retried, long failed)>[insertionWorkersCount];
            _retryWorkers = new Task<(long processed, long retried, long failed)>[retryWorkersCount];
            
            _retriesChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
        }
        
        public void Start(CancellationToken token)
        {
            for (var workerN = 0; workerN < _insertionWorkers.Length; workerN++)
            {
                _insertionWorkers[workerN] = RunWorker(_retriesChannel.Writer,
                    _logger.ForContext("Scope", $"{_collectionName}-{workerN:00}"), token);
            }

            for (var retryWorkerN = 0; retryWorkerN < _retryWorkers.Length; retryWorkerN++)
            {
                _retryWorkers[retryWorkerN] = RunRetryWorker(_retriesChannel.Reader,
                    _logger.ForContext("Scope", $"{_collectionName}-Retry{retryWorkerN:00}"), token);
            }

            _logger.Debug("Started {I} insertion worker(s) and {R} retry worker(s)", _insertionWorkers.Length, _retryWorkers.Length);
        }

        public async Task<(long processed, long retried, long failed)> StopAsync()
        {
            _logger.Debug("Finished reading documents, waiting for insertion-workers");
            await Task.WhenAll(_insertionWorkers);
            
            _logger.Debug("Insertion-workers finished, waiting for retry-workers");
            _retriesChannel.Writer.Complete();
            await Task.WhenAll(_retryWorkers);

            return await GetResultsAsync();
        }
        
        private async Task<(long processed, long totalRetried, long failed)> RunWorker(ChannelWriter<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token)
        {
            var sw = new Stopwatch();
            
            var totalProcessed = 0L;
            var totalRetried = 0L;
            var totalFailed = 0L;
            
            await foreach (var (count, batch) in _batchReader.ReadAllAsync(token))
            {
                var requests = batch.Take(count).ToArray();
                try
                {
                    if (!_dryRun)
                    {
                        sw.Restart();
                        var results = await _collection.BulkWriteAsync(requests, new BulkWriteOptions
                        {
                            IsOrdered = true,
                            BypassDocumentValidation = true
                        }, token);
                        sw.Stop();

                        var processedCount = GetSuccessfulOperationsCount(results);

                        workerLogger.Debug("Processed {s} documents from batch of size {count} in {elapsed:N1} ms",
                            processedCount, count, sw.ElapsedMilliseconds);

                        totalProcessed += processedCount;
                    }
                    _notifier.Notify(count);
                    ArrayPool<WriteModel<BsonDocument>>.Shared.Return(batch);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (MongoBulkWriteException<BsonDocument> bwe)
                {
                    var retries = 0;
                    var fails = 0;
                    foreach (var error in bwe.WriteErrors)
                    {
                        switch (error.Message)
                        {
                            case ErrorUpdateWithMoveToAnotherShard:
                                await failedWrites.WriteAsync(requests[error.Index], token);
                                retries++;
                                break;
                            default:
                                fails++;
                                break;
                        }
                    }
                    
                    totalFailed += fails;
                    totalRetried += retries;
                    totalProcessed += GetSuccessfulOperationsCount(bwe.Result);
                    
                    workerLogger.Error("{N} documents failed to transfer, {R} were sent to retry", fails, retries);
                    workerLogger.Debug(bwe, "Bulk write exception details:");
                }
                catch (Exception e)
                {
                    workerLogger.Error(e, "Error occurred while transferring documents");
                }
            } 
            
            return (totalProcessed, totalRetried, totalFailed);
        }

        private static long GetSuccessfulOperationsCount(BulkWriteResult results) =>
            results.MatchedCount + results.Upserts.Count;

        private async Task<(long processed, long totalRetried, long failed)> RunRetryWorker(ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token)
        {
            var sw = new Stopwatch();
            
            var totalProcessed = 0L;
            var totalFailed = 0L;
            
            await foreach (var failedReplace in failedWrites.ReadAllAsync(token))
            {
                var documentId = failedReplace.Replacement["_id"].AsObjectId;
                try
                {
                    if (!_dryRun)
                    {
                        sw.Restart();
                        await _collection.ReplaceOneAsync(failedReplace.Filter, failedReplace.Replacement, new ReplaceOptions
                        {
                            IsUpsert = _upsert,
                            BypassDocumentValidation = true
                        }, token);
                        sw.Stop();
                        
                        workerLogger.Debug("Successfully retried replacement of document (ID: {id}) in {elapsed:N1} ms", documentId, sw.ElapsedMilliseconds);

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
                    workerLogger.Error("Failed to retry replacement (ID: {id}): {msg}", documentId, we.Message);
                    workerLogger.Debug(we, "Retry exception details:");
                    totalFailed++;
                }
                catch (Exception e)
                {
                    workerLogger.Error(e, "Error occurred while transferring documents to {collection}", _collectionName);
                }
            }
            
            return (totalProcessed, 0, totalFailed);
        }

        private async Task<(long processed, long retried, long failed)> GetResultsAsync()
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

            return (processed, retried, failed);
        }
    }
}