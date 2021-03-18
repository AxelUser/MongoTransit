using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransit.Storage.Source
{
    public interface ISourceRepository
    {
        Task ReadDocumentsAsync(FilterDefinition<BsonDocument> filter,
            ChannelWriter<List<ReplaceOneModel<BsonDocument>>> batchWriter,
            int batchSize,
            bool fetchKeyFromDestination,
            string[] keyFields,
            bool upsert,
            IDocumentFinder documentFinder,
            CancellationToken token);

        Task<long> CountLagAsync(string field, DateTime checkpoint, CancellationToken token);
        Task<long> CountAllDocumentsAsync(CancellationToken token);
    }
}