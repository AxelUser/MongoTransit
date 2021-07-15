using MongoDB.Bson;

namespace MongoTransit.IntegrationTests.Extensions
{
    public static class BsonDocumentExtensions
    {
        public static BsonDocument GetFilterBy(this BsonDocument document, string key)
        {
            return new BsonDocument(key, document[key]);
        }
    }
}