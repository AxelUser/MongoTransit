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
            _fromCollection = new MongoClient(options.FromConnectionString).GetDatabase(options.Database)
                .GetCollection<BsonDocument>(options.Collection);

            // TODO check DB for existence
            // TODO check collection for existence
            _toCollection = new MongoClient(options.ToConnectionString).GetDatabase(options.Database)
                .GetCollection<BsonDocument>(options.Collection);
        }
        
        public async Task TransitAsync(bool dryRun, CancellationToken token)
        {
            _logger.Debug("Starting transit operation");
            
            var transitChannel = Channel.CreateBounded<(int count, WriteModel<BsonDocument>[] batch)>(_options.Workers);

            var sw = new Stopwatch();
            sw.Start();
            var (filter, count) = await CheckCollectionAsync(token);
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
            try
            {
                var insertionWorkers = new Task<(long processed, long failed)>[_options.Workers * Environment.ProcessorCount];
                _logger.Debug("Starting {N} insertion workers", insertionWorkers.Length);
                for (var workerN = 0; workerN < insertionWorkers.Length; workerN++)
                {
                    insertionWorkers[workerN] = RunWorker(notifier, transitChannel.Reader,
                        _logger.ForContext("Scope", $"{_options.Collection}-{workerN:00}"), dryRun, token);
                }

                _logger.Debug("Started {N} insertion workers", insertionWorkers.Length);

                _logger.Debug("Started reading documents from source");
                await ReadDocumentsAsync(_options.BatchSize, _options.UpsertFields, fromCursor, transitChannel.Writer,
                    token);

                _logger.Debug("Finished reading documents, waiting for insertion completion");
                await Task.WhenAll(insertionWorkers);
                _logger.Debug("All workers finished inserting");

                await LogTotalResults(insertionWorkers);
            }
            finally
            {
                sw.Stop();
                _logger.Debug("Transit finished in {elapsed}", sw.Elapsed);
                _manager.Detach(_options.Collection);
            }
        }

        private async Task LogTotalResults(Task<(long processed, long failed)>[] insertionWorkers)
        {
            var processed = 0L;
            var failed = 0L;

            foreach (var worker in insertionWorkers)
            {
                var (s, f) = await worker;
                processed += s;
                failed += f;
            }

            _logger.Information("Transferred {S} documents; Failed {F} documents", processed, failed);
        }

        private async Task<(BsonDocument filter, long count)> CheckCollectionAsync(CancellationToken token)
        {
            if (_options.IterativeTransferOptions != null)
            {
                return await CheckIterativeCollectionAsync(_options.IterativeTransferOptions, token);
            }

            var filter = new BsonDocument();
            var count = await _fromCollection.CountDocumentsAsync(filter, cancellationToken: token);
            return (filter, count);
        }

        private async Task<(BsonDocument filter, long count)> CheckIterativeCollectionAsync(IterativeTransitOptions iterOpts, CancellationToken token)
        {
            _logger.Debug("Detected iterative transit option. Fetching checkpoint and lag");

            var (checkpointField, forcedCheckpoint) = iterOpts;

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
            var count = await CountLagAsync(checkpointField, lastCheckpoint.Value, token);

            _logger.Debug("Collection {Collection} has checkpoint {lastCheckpoint} and lag {lag:N0}",
                _options.Collection, lastCheckpoint, count);
            
            var filter = new BsonDocument(checkpointField, new BsonDocument("$gte", lastCheckpoint));

            return (filter, count);
        }

        private async Task ReadDocumentsAsync(int batchSize, string[] upsertFields, IAsyncCursor<BsonDocument> documentsReader,
            ChannelWriter<(int count, WriteModel<BsonDocument>[] batch)> batchWriter, CancellationToken token)
        {
            WriteModel<BsonDocument>[] batch = ArrayPool<WriteModel<BsonDocument>>.Shared.Rent(batchSize);
            var count = 0;
            await documentsReader.ForEachAsync(async document =>
            {
                if (count < batchSize)
                {
                    var filter = new BsonDocument();
                    foreach (var field in upsertFields)
                    {
                        filter[field] = document[field];
                    }

                    batch[count] = new ReplaceOneModel<BsonDocument>(filter, document)
                    {
                        IsUpsert = true
                    };
                    count++;
                }
                else
                {
                    _logger.Debug("Prepared batch of size {count}", count);
                    await batchWriter.WriteAsync((count, batch), token);
                    count = 0;
                    batch = ArrayPool<WriteModel<BsonDocument>>.Shared.Rent(batchSize);
                }
            }, token);

            // Handle case when cursor is finished, but there are some documents less than batch.
            if (count > 0)
            {
                _logger.Debug("Flushing the remaining batch of size {count}", count);
                // Flush remaining documents
                await batchWriter.WriteAsync((count, batch), token);
            }
            batchWriter.Complete();
        }

        private async Task<(long processed, long failed)> RunWorker(ProgressNotifier notifier,
            ChannelReader<(int count, WriteModel<BsonDocument>[] batch)> batchReader,
            ILogger workerLogger,
            bool dryRun,
            CancellationToken token)
        {
            var sw = new Stopwatch();
            
            var totalProcessed = 0L;
            var totalFailed = 0L;
            
            await foreach (var (count, batch) in batchReader.ReadAllAsync(token))
            {
                try
                {
                    workerLogger.Debug("Received batch of size {count}", count);
                    if (!dryRun)
                    {
                        sw.Restart();
                        var results = await _toCollection.BulkWriteAsync(batch.Take(count), new BulkWriteOptions
                        {
                            IsOrdered = false,
                            BypassDocumentValidation = true
                        }, token);
                        sw.Stop();

                        var processedCount = GetSuccessfulOperationsCount(results);

                        _logger.Debug("Processed {s} documents from batch of size {count} in {elapsed:N1} ms",
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
                    _logger.Error("{N} documents failed to transfer in {collection}", bwe.WriteErrors.Count, _options.Collection);
                    _logger.Debug(bwe, "Bulk write exception details:");
                    totalFailed += bwe.WriteErrors.Count;
                    totalProcessed += GetSuccessfulOperationsCount(bwe.Result);
                }
                catch (Exception e)
                {
                    _logger.Error(e, "Error occurred while transferring documents to {collection}", _options.Collection);
                }
            }

            return (totalProcessed, totalFailed);
        }

        private static long GetSuccessfulOperationsCount(BulkWriteResult results) =>
            results.MatchedCount + results.Upserts.Count;

        private static async Task<DateTime?> FindCheckpointAsync(IMongoCollection<BsonDocument> collection, string checkpointField, CancellationToken token)
        {
            var checkpointBson = await (await collection.FindAsync(new BsonDocument
            {
                [checkpointField] = new BsonDocument("$exists", true)
            }, new FindOptions<BsonDocument>
            {
                Sort = new BsonDocument(checkpointField, 1),
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