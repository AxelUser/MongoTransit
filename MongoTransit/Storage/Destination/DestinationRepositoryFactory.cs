using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Storage.Destination
{
    public class DestinationRepositoryFactory : IDestinationRepositoryFactory
    {
        private readonly IMongoCollection<BsonDocument> _collection;

        public DestinationRepositoryFactory(string connectionString, string database, string collectionName)
        {
            // TODO check DB for existence
            // TODO check collection for existence
            _collection = new MongoClient(connectionString).GetDatabase(database)
                .GetCollection<BsonDocument>(collectionName);
        }

        public IDestinationRepository Create(ILogger logger)
        {
            return new DestinationRepository(_collection, logger);
        }
    }
}