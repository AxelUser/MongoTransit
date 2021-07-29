using MongoDB.Bson;

namespace MongoTransit.IntegrationTests
{
    public class Entity
    {
        public string ShardedKey { get; set; }

        public string Value { get; set; }
    }
}