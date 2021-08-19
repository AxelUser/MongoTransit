using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace MongoTransit.Storage
{
    public interface IDestinationDocumentFinder
    {
        public Task<BsonDocument?> FindDocumentAsync(BsonDocument document, CancellationToken token);
    }
}