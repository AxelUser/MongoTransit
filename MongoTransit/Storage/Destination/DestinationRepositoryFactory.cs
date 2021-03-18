using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Storage.Destination
{
    public class DestinationRepositoryFactory : IDestinationRepositoryFactory
    {
        private readonly string _connectionString;
        private readonly string _database;
        private readonly string _collectionName;

        public DestinationRepositoryFactory(string connectionString, string database, string collectionName)
        {
            _connectionString = connectionString;
            _database = database;
            _collectionName = collectionName;
        }

        public IDestinationRepository Create(ILogger logger)
        {
            // TODO check DB for existence
            // TODO check collection for existence
            var collection = new MongoClient(_connectionString).GetDatabase(_database)
                .GetCollection<BsonDocument>(_collectionName);
            return new DestinationRepository(collection, logger);
        }
    }
}