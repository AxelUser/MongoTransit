using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransit
{
    public class CollectionTransitHandler
    {
        private readonly CollectionTransitOptions _options;
        private readonly IMongoCollection<BsonDocument> _fromCollection;
        private readonly IMongoCollection<BsonDocument> _toCollection;

        public CollectionTransitHandler(CollectionTransitOptions options)
        {
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
        
        public async Task TransitAsync(CancellationToken token)
        {
            var transitChannel = Channel.CreateBounded<(int count, WriteModel<BsonDocument>[] batch)>(_options.Workers);
            
            IAsyncCursor<BsonDocument> fromCursor = await CreateReadingCursorAsync(token);
            
            var insertionWorkers = new List<Task>(_options.Workers);
            for (var workerN = 0; workerN < _options.Workers; workerN++)
            {
                insertionWorkers[workerN] = RunWorker(transitChannel.Reader, token);
            }
            
            await ReadDocumentsAsync(_options.BatchSize, _options.UpsertFields, fromCursor, transitChannel.Writer, token);

            await Task.WhenAll(insertionWorkers);
        }

        private static async Task ReadDocumentsAsync(int batchSize, string[] upsertFields, IAsyncCursor<BsonDocument> documentsReader,
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
                    await batchWriter.WriteAsync((count, batch), token);
                    count = 0;
                    batch = ArrayPool<WriteModel<BsonDocument>>.Shared.Rent(batchSize);
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

        private async Task RunWorker(ChannelReader<(int count, WriteModel<BsonDocument>[] batch)> batchReader, CancellationToken token)
        {
            await foreach (var (count, batch) in batchReader.ReadAllAsync(token))
            {
                await _toCollection.BulkWriteAsync(batch.Take(count), new BulkWriteOptions
                {
                    IsOrdered = false,
                    BypassDocumentValidation = true
                }, token);
                
                ArrayPool<WriteModel<BsonDocument>>.Shared.Return(batch);
            }
        }

        private async Task<IAsyncCursor<BsonDocument>> CreateReadingCursorAsync(CancellationToken token)
        {
            if (_options.IterativeTransferOptions == null)
                // Full collection transition
                return await _fromCollection.FindAsync(null, cancellationToken: token);
            
            var checkpointField = _options.IterativeTransferOptions.Field;
            var lastCheckpoint = _options.IterativeTransferOptions?.ForcedCheckpoint ??
                                 await FindCheckpointAsync(_toCollection, checkpointField, token);

            // TODO handle if last checkpoint is null
            var filter = new BsonDocument(checkpointField, new BsonDocument("$gte", lastCheckpoint));
            return await _fromCollection.FindAsync(filter, cancellationToken: token);
        }

        private static async Task<DateTime?> FindCheckpointAsync(IMongoCollection<BsonDocument> collection, string checkpointField, CancellationToken token)
        {
            var checkpointBson = await collection.FindSync(new BsonDocument
            {
                [checkpointField] = new BsonDocument("$exists", true)
            }, new FindOptions<BsonDocument>
            {
                Sort = new BsonDocument(checkpointField, -1),
                Limit = 1,
                Projection = new BsonDocument
                {
                    ["_id"] = false,
                    [checkpointField] = true
                }
            }, token).SingleOrDefaultAsync(token);

            if (checkpointBson == null)
            {
                return null;
            }

            return checkpointBson[checkpointField].ToUniversalTime();
        }
    }
}