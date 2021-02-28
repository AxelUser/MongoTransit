using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransit
{
    public static class CollectionTransitHandler
    {
        public static async Task RunAsync(CollectionTransitOptions options, CancellationToken token)
        {
            var fromClient = new MongoClient(options.FromConnectionString);
            
            // TODO check DB for existence
            var fromDb = fromClient.GetDatabase(options.Database);

            // TODO check collection for existence
            var fromCollection = fromDb.GetCollection<BsonDocument>(options.Collection);
            
            var toClient = new MongoClient(options.FromConnectionString);
            
            // TODO check DB for existence
            var toDb = fromClient.GetDatabase(options.Database);

            // TODO check collection for existence
            var toCollection = fromDb.GetCollection<BsonDocument>(options.Collection);

            var checkpointField = options.IterativeTransferOptions.Field;
            var lastCheckpoint = options.IterativeTransferOptions?.ForcedCheckpoint ??
                                 await FindCheckpointAsync(toCollection, checkpointField, token);
            
            // TODO handle if last checkpoint is null
            var filter = new BsonDocument(checkpointField, new BsonDocument("$gte", lastCheckpoint));
            var fromCursor = await fromCollection.FindAsync(filter, cancellationToken: token);

            await fromCursor.ForEachAsync(async document =>
            {
                var filter = new BsonDocument();
                foreach (var upsertField in options.UpsertFields)
                {
                    filter[upsertField] = document[upsertField];
                }
                await toCollection.UpdateOneAsync(filter, document, new UpdateOptions
                {
                    IsUpsert = true,
                    BypassDocumentValidation = true
                }, token);
            }, token);
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