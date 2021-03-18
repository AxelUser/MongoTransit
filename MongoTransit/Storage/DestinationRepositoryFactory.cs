using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Storage
{
    public class DestinationRepositoryFactory
    {
        private readonly IMongoCollection<BsonDocument> _collection;

        public DestinationRepositoryFactory(IMongoCollection<BsonDocument> collection)
        {
            _collection = collection;
        }

        public DestinationRepository Create(ILogger logger)
        {
            return new DestinationRepository(_collection, logger);
        }
    }
}