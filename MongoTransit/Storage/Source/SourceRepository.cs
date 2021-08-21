using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Storage.Source.Models;
using Serilog;

namespace MongoTransit.Storage.Source
{
    public class SourceRepository : ISourceRepository
    {
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly ILogger _logger;

        public SourceRepository(IMongoCollection<BsonDocument> collection, ILogger logger)
        {
            _collection = collection;
            _logger = logger;
        }

        public async Task ReadDocumentsAsync(SourceFilter filter,
            ChannelWriter<List<ReplaceOneModel<BsonDocument>>> batchWriter,
            int batchSize,
            string[] keyFields,
            bool upsert,
            IDestinationDocumentFinder? documentFinder,
            CancellationToken token)
        {
            _logger.Debug("Creating a cursor to the source with batch size {Batch}", batchSize);
            using var cursor = await _collection.FindAsync(
                filter.ToBsonDocument(), new FindOptions<BsonDocument>
                {
                    BatchSize = batchSize
                }, token);
            _logger.Debug("Started reading documents from source");
            try
            {
                while (await cursor.MoveNextAsync(token))
                {
                    if (cursor.Current == null || !cursor.Current.Any())
                        continue;
                    var currentBatch = cursor.Current.ToList();

                    var destinationDocuments = new Dictionary<BsonValue, BsonDocument>();
                    if (documentFinder != null)
                    {
                        var dest = await documentFinder.FindDocumentsAsync(currentBatch, token);
                        foreach (var doc in dest)
                        {
                            destinationDocuments[doc["_id"]] = doc;
                        }
                    }

                    var replaceModels = currentBatch.Select(srcDoc => CreateReplaceModel(
                            destinationDocuments.TryGetValue(srcDoc["_id"], out var destDoc)
                                ? destDoc
                                : srcDoc, srcDoc, keyFields, upsert))
                        .ToList();

                    await batchWriter.WriteAsync(replaceModels, token);
                }
            }
            finally
            {
                batchWriter.Complete();
            }
        }
        
        public async Task<long> CountLagAsync(SourceFilter filter, CancellationToken token)
        {
            return await _collection.CountDocumentsAsync(filter.ToBsonDocument(), cancellationToken: token);
        }

        public async Task<long> CountAllAsync(CancellationToken token)
        {
            return await _collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken: token);
        }

        private static ReplaceOneModel<BsonDocument> CreateReplaceModel(BsonDocument filterFields, BsonDocument replacement,
            string[] keyFields,
            bool upsert)
        {
            var filter = new BsonDocumentFilterDefinition<BsonDocument>(new BsonDocument());
            if (keyFields.Any())
            {
                foreach (var field in keyFields)
                {
                    filter.Document[field] = filterFields[field];
                }
            }
            else
            {
                filter.Document["_id"] = filterFields["_id"];
            }

            var model = new ReplaceOneModel<BsonDocument>(filter, replacement)
            {
                IsUpsert = upsert
            };
            return model;
        }
    }
}