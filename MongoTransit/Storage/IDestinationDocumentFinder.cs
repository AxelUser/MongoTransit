using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace MongoTransit.Storage
{
    public interface IDestinationDocumentFinder
    {
        public Task<List<BsonDocument>> FindDocumentsAsync(IReadOnlyCollection<BsonDocument> documents,
            CancellationToken token);
    }
}