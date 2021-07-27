using MongoDB.Bson;

namespace MongoTransit.IntegrationTests
{
    public class Entity
    {
        public ObjectId Id { get; set; }

        public string ShardedKey { get; set; }

        public string Value { get; set; }
    }
}