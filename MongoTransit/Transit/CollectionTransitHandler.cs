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
    public class CollectionTransitHandler
    {
        private const string ErrorUpdateWithMoveToAnotherShard =
            "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1";
        
        private readonly ProgressManager _manager;
        private readonly ILogger _logger;
        private readonly CollectionTransitOptions _options;
        private readonly IMongoCollection<BsonDocument> _fromCollection;
        private readonly IMongoCollection<BsonDocument> _toCollection;

        public CollectionTransitHandler(ProgressManager manager, ILogger logger, CollectionTransitOptions options)
        {
            _manager = manager;
            _logger = logger;
            _options = options;
            
            // TODO check DB for existence
            // TODO check collection for existence
            _fromCollection = new MongoClient(options.SourceConnectionString).GetDatabase(options.Database)
                .GetCollection<BsonDocument>(options.Collection);

            // TODO check DB for existence
            // TODO check collection for existence
            _toCollection = new MongoClient(options.DestinationConnectionString).GetDatabase(options.Database)
                .GetCollection<BsonDocument>(options.Collection);
        }
        
        public async Task TransitAsync(bool dryRun, CancellationToken token)
        {
            _logger.Debug("Starting transit operation");
            var swTransit = new Stopwatch();
            swTransit.Start();

            var status = new TextStatusProvider("Checking...");
            _manager.Attach(_options.Collection, status);
            try
            {
                await InternalTransit(dryRun, status, token);
            }
            finally
            {
                swTransit.Stop();
                _logger.Debug("Transit finished in {elapsed}", swTransit.Elapsed);
                _manager.Detach(_options.Collection);
            }
        }

        private async Task InternalTransit(bool dryRun, TextStatusProvider progress, CancellationToken token)
        {
            var sw = new Stopwatch();
            var transitChannel = Channel.CreateBounded<(int count, ReplaceOneModel<BsonDocument>[] batch)>(_options.Workers);
            var retriesChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
            var (filter, count) = await CheckCollectionAsync(progress, token);
            sw.Stop();
            _logger.Debug("Collection check was completed in {elapsed} ms", sw.ElapsedMilliseconds);

            if (count == 0)
            {
                _logger.Information("Collection {Collection} is up-to date, skipping transit", _options.Collection);
                return;
            }

            _logger.Debug("Creating cursor to source");
            IAsyncCursor<BsonDocument> fromCursor = await _fromCollection.FindAsync(
                filter, new FindOptions<BsonDocument>
                {
                    BatchSize = _options.BatchSize
                }, token);

            var notifier = new ProgressNotifier(count);
            _manager.Attach(_options.Collection, notifier);
            
            sw.Restart();

            var insertionWorkersCount = _options.Workers * Environment.ProcessorCount;
            var retryWorkersCount = 1;
            var workers = new List<Task<(long processed, long retried, long failed)>>();
            var retriers = new List<Task<(long processed, long retried, long failed)>>();
            
            for (var workerN = 0; workerN < insertionWorkersCount; workerN++)
            {
                workers.Add(RunWorker(notifier, transitChannel.Reader, retriesChannel.Writer,
                    _logger.ForContext("Scope", $"{_options.Collection}-{workerN:00}"), dryRun, token));
            }

            for (var retryWorkerN = 0; retryWorkerN < retryWorkersCount; retryWorkerN++)
            {
                retriers.Add(RunRetryWorker(notifier, retriesChannel.Reader,
                    _logger.ForContext("Scope", $"{_options.Collection}-Retry{retryWorkerN:00}"), dryRun, token));
            }

            _logger.Debug("Started {I} insertion worker(s) and {R} retry worker(s)", insertionWorkersCount, retryWorkersCount);

            _logger.Debug("Started reading documents from source");
            await ReadDocumentsAsync(fromCursor, transitChannel.Writer, token);

            _logger.Debug("Finished reading documents, waiting for insertion-workers");
            await Task.WhenAll(workers);
            
            _logger.Debug("Insertion-workers finished, waiting for retry-workers");
            retriesChannel.Writer.Complete();
            await Task.WhenAll(retriers);
            
            _logger.Debug("Transfer was completed in {elapsed}", sw.Elapsed);

            await LogTotalResults(workers.Concat(retriers));
        }

        private async Task LogTotalResults(IEnumerable<Task<(long processed, long retried, long failed)>> insertionWorkers)
        {
            var processed = 0L;
            var retried = 0L;
            var failed = 0L;

            foreach (var worker in insertionWorkers)
            {
                var (s, r, f) = await worker;
                processed += s;
                retried += r;
                failed += f;
            }

            _logger.Information("Transferred {S}; Retried {R}; Failed {F};", processed, retried, failed);
        }

        private async Task<(BsonDocument filter, long count)> CheckCollectionAsync(TextStatusProvider progress,
            CancellationToken token)
        {
            if (_options.IterativeTransferOptions != null)
            {
                return await CheckIterativeCollectionAsync(progress, _options.IterativeTransferOptions, token);
            }

            var filter = new BsonDocument();
            progress.Status = "Counting documents...";
            var count = await _fromCollection.CountDocumentsAsync(filter, cancellationToken: token);
            return (filter, count);
        }

        private async Task<(BsonDocument filter, long count)> CheckIterativeCollectionAsync(TextStatusProvider progress, IterativeTransitOptions iterOpts, CancellationToken token)
        {
            _logger.Debug("Detected iterative transit option. Fetching checkpoint and lag");

            var (checkpointField, forcedCheckpoint) = iterOpts;

            progress.Status = "Searching checkpoint...";

            DateTime? lastCheckpoint;
            
            if (forcedCheckpoint != null)
            {
                _logger.Information("Forced to use checkpoint {ForcedCheckpoint} for collection {Collection}",
                    iterOpts.ForcedCheckpoint, _options.Collection);
                lastCheckpoint = iterOpts.ForcedCheckpoint;
            }
            else
            {
                _logger.Debug("Fetching last checkpoint for collection {Collection}", _options.Collection);
                lastCheckpoint = await FindCheckpointAsync(_toCollection, checkpointField, token);
            }
            
            if (lastCheckpoint == null)
            {
                throw new Exception($"Couldn't get checkpoint for collection {_options.Collection}");
            }

            _logger.Debug("Counting how many documents should be transferred");
            progress.Status = "Counting documents...";
            var count = await CountLagAsync(checkpointField, lastCheckpoint.Value, token);

            _logger.Debug("Collection {Collection} has checkpoint {lastCheckpoint} and lag {lag:N0}",
                _options.Collection, lastCheckpoint, count);
            
            var filter = new BsonDocument(checkpointField, new BsonDocument("$gte", lastCheckpoint));

            return (filter, count);
        }

        private async Task ReadDocumentsAsync(IAsyncCursor<BsonDocument> documentsReader,
            ChannelWriter<(int count, ReplaceOneModel<BsonDocument>[] batch)> batchWriter, CancellationToken token)
        {
            var batch = ArrayPool<ReplaceOneModel<BsonDocument>>.Shared.Rent(_options.BatchSize);
            var count = 0;
            await documentsReader.ForEachAsync(async document =>
            {
                if (count < _options.BatchSize)
                {
                    var fields = document;
                    if (_options.FetchKeyFromDestination)
                    {
                        // TODO maybe defer and put it in batch
                        var foundDestinationDoc = await (await _toCollection.FindAsync(new BsonDocument("_id", document["_id"]),
                            cancellationToken: token)).SingleOrDefaultAsync(token);
                        if (foundDestinationDoc != null)
                        {
                            fields = foundDestinationDoc;
                        }
                    }
                    var filter = new BsonDocument();
                    if (_options.KeyFields?.Any() == true)
                    {
                        foreach (var field in _options.KeyFields)
                        {
                            filter[field] = fields[field];
                        }   
                    }
                    else
                    {
                        filter["_id"] = fields["_id"];
                    }

                    batch[count] = new ReplaceOneModel<BsonDocument>(filter, document)
                    {
                        IsUpsert = _options.Upsert
                    };
                    count++;
                }
                else
                {
                    await batchWriter.WriteAsync((count, batch), token);
                    count = 0;
                    batch = ArrayPool<ReplaceOneModel<BsonDocument>>.Shared.Rent(_options.BatchSize);
                }
            }, token);

            // Handle case when cursor is finished, but there are some documents less than batch.
            if (count > 0)
            {
                // Flush remaining documents
                await batchWriter.WriteAsync((count, batch), token);
            }
            batchWriter.Complete();
        }

        private async Task<(long processed, long totalRetried, long failed)> RunWorker(ProgressNotifier notifier,
            ChannelReader<(int count, ReplaceOneModel<BsonDocument>[] batch)> batchReader,
            ChannelWriter<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            bool dryRun,
            CancellationToken token)
        {
            var sw = new Stopwatch();
            
            var totalProcessed = 0L;
            var totalRetried = 0L;
            var totalFailed = 0L;
            
            await foreach (var (count, batch) in batchReader.ReadAllAsync(token))
            {
                var requests = batch.Take(count).ToArray();
                try
                {
                    if (!dryRun)
                    {
                        sw.Restart();
                        var results = await _toCollection.BulkWriteAsync(requests, new BulkWriteOptions
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
                    notifier.Notify(count);
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
                    workerLogger.Error(e, "Error occurred while transferring documents to {collection}", _options.Collection);
                }
            }

            return (totalProcessed, totalRetried, totalFailed);
        }

        private async Task<(long processed, long totalRetried, long failed)> RunRetryWorker(ProgressNotifier notifier,
            ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            bool dryRun,
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
                    if (!dryRun)
                    {
                        sw.Restart();
                        await _toCollection.ReplaceOneAsync(failedReplace.Filter, failedReplace.Replacement, new ReplaceOptions
                        {
                            IsUpsert = _options.Upsert,
                            BypassDocumentValidation = true
                        }, token);
                        sw.Stop();
                        
                        workerLogger.Debug("Successfully retried replacement of document (ID: {id}) in {elapsed:N1} ms", documentId, sw.ElapsedMilliseconds);

                        totalProcessed++;
                    }
                    notifier.Notify(1);
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
                    workerLogger.Error(e, "Error occurred while transferring documents to {collection}", _options.Collection);
                }
            }
            
            return (totalProcessed, 0, totalFailed);
        }

        private static long GetSuccessfulOperationsCount(BulkWriteResult results) =>
            results.MatchedCount + results.Upserts.Count;

        private static async Task<DateTime?> FindCheckpointAsync(IMongoCollection<BsonDocument> collection, string checkpointField, CancellationToken token)
        {
            var checkpointBson = await (await collection.FindAsync(new BsonDocument
            {
                [checkpointField] = new BsonDocument
                {
                    ["$exists"] = true,
                    ["$ne"] = BsonNull.Value
                }
            }, new FindOptions<BsonDocument>
            {
                Sort = new BsonDocument(checkpointField, -1),
                Limit = 1,
                Projection = new BsonDocument
                {
                    ["_id"] = false,
                    [checkpointField] = true
                }
            }, token)).SingleOrDefaultAsync(token);

            if (checkpointBson == null)
            {
                return null;
            }

            return checkpointBson[checkpointField].ToUniversalTime();
        }

        private async Task<long> CountLagAsync(string field, DateTime checkpoint, CancellationToken token)
        {
            var filter = new BsonDocument
            {
                [field] = new BsonDocument("$gt", checkpoint)
            };

            return await _fromCollection.CountDocumentsAsync(filter, cancellationToken: token);
        }
    }
}