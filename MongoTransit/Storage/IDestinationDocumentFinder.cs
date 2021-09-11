using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace MongoTransit.Storage
{
    public interface IDestinationDocumentFinder
    {
        public Task<List<BsonDocument>> GetFieldsAsync(IReadOnlyCollection<BsonDocument> targetedDocuments,
            string[] fields,
            CancellationToken token);
    }
}