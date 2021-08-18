using System;

namespace MongoTransit.IntegrationTests
{
    public class Entity
    {
        public string Id { get; set; }
        public long ShardedKey { get; set; }

        public string Value { get; set; }

        public DateTime Modified { get; set; }
    }
}