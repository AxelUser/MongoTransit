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
        Task ReadDocumentsAsync(SourceFilter filter,
            ChannelWriter<List<ReplaceOneModel<BsonDocument>>> batchWriter,
            int batchSize,
            bool fetchKeyFromDestination,
            string[] keyFields,
            bool upsert,
            IDestinationDocumentFinder documentFinder,
            CancellationToken token = default);

        Task<long> CountLagAsync(SourceFilter filter, CancellationToken token);
        
        Task<long> CountAllAsync(CancellationToken token);
    }
}