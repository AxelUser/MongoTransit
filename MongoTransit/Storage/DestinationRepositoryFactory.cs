using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Storage
{
    public class DestinationRepositoryFactory
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

        public DestinationRepository Create(ILogger logger)
        {
            return new DestinationRepository(_connectionString, _database, _collectionName, logger);
        }
    }
}