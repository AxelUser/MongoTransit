using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;

namespace MongoTransit.Extensions
{
    public static class BsonDocumentExtensions
    {
        public static BsonDocument GetFilterBy(this BsonDocument document, string key)
        {
            return new BsonDocument(key, document[key]);
        }
        
        public static BsonDocument GetInFilterBy(this IReadOnlyCollection<BsonDocument> documents, string key)
        {
            var keys = new BsonArray(documents.Select(d => d[key]));
            return new BsonDocument(key, new BsonDocument("$in", keys));
        }
    }
}