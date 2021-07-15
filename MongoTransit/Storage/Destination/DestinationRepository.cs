using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Storage.Destination
{
    public class DestinationRepository: IDestinationRepository
    {
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly ILogger _logger;

        public DestinationRepository(IMongoCollection<BsonDocument> collection, ILogger logger)
        {
            _collection = collection;
            _logger = logger;
        }

        public async Task ReplaceManyAsync(List<ReplaceOneModel<BsonDocument>>? replacements, CancellationToken token)
        {
            if (replacements == null || !replacements.Any())
            {
                _logger.Debug("Empty bulk, skipping");
                return;
            }
            
            var sw = new Stopwatch();
            sw.Start();
            await _collection.BulkWriteAsync(replacements, new BulkWriteOptions
            {
                IsOrdered = false,
                BypassDocumentValidation = true
            }, token);
            sw.Stop();
            
            _logger.Debug("Processed bulk of {Count:N0} documents in {Elapsed:N1} ms",
                replacements.Count, sw.ElapsedMilliseconds);
        }

        public async Task ReplaceDocumentAsync(FilterDefinition<BsonDocument> filter, BsonDocument replacement, CancellationToken token)
        {
            var sw = new Stopwatch();
            sw.Start();
            var result = await _collection.ReplaceOneAsync(filter, replacement, new ReplaceOptions
            {
                BypassDocumentValidation = true
            }, token);
            sw.Stop();

            _logger.Debug(
                result.ModifiedCount == 1
                    ? "Successfully retried replacement of document (ID: {Id}) in {Elapsed:N1} ms"
                    : "Failed replacement of document (ID: {Id}) in {Elapsed:N1} ms. Document is missing",
                replacement["_id"], sw.ElapsedMilliseconds);
        }

        public async Task DeleteAllDocumentsAsync(CancellationToken token)
        {
            var sw = new Stopwatch();
            sw.Start();
            await _collection.DeleteManyAsync(new BsonDocument(), token);
            sw.Stop();
            _logger.Debug("Removed all documents at collection in {Elapsed}", sw.Elapsed);
        }
        
        public async Task<DateTime?> FindLastCheckpointAsync(string checkpointField, CancellationToken token)
        {
            var checkpointBson = await (await _collection.FindAsync(new BsonDocument
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
                    ["_id"] = true,
                    [checkpointField] = true
                }
            }, token)).SingleOrDefaultAsync(token);

            if (checkpointBson == null)
            {
                return null;
            }

            return checkpointBson[checkpointField].ToUniversalTime();
        }

        public async Task<BsonDocument> FindDocumentAsync(BsonDocument document, CancellationToken token)
        {
            var cursor = await _collection.FindAsync(new BsonDocument("_id", document["_id"]),
                cancellationToken: token);
            return await cursor.SingleOrDefaultAsync(token);
        }
    }
}