using System;
using System.Buffers;
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

            var workerPool = new WorkerPool(_options.Workers * Environment.ProcessorCount,
                _options.Workers * Environment.ProcessorCount,
                _toCollection, transitChannel, notifier, _options.Upsert, dryRun, _logger);
            
            sw.Restart();

            workerPool.Start(token);
            
            _logger.Debug("Started reading documents from source");
            await ReadDocumentsAsync(fromCursor, transitChannel.Writer, token);

            var (processed, retried, failed) = await workerPool.StopAsync();
            
            _logger.Debug("Transfer was completed in {elapsed}", sw.Elapsed);

            
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