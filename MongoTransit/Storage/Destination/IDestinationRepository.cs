using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransit.Storage.Destination
{
    public interface IDestinationRepository: IDestinationDocumentFinder
    {
        Task ReplaceManyAsync(List<ReplaceOneModel<BsonDocument>>? replacements, CancellationToken token);
        Task ReplaceDocumentAsync(ReplaceOneModel<BsonDocument> model, CancellationToken token);
        Task DeleteAllDocumentsAsync(CancellationToken token);
        Task<DateTime?> FindLastCheckpointAsync(string checkpointField, CancellationToken token);
    }
}