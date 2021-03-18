using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Storage.Source
{
    public class SourceRepositoryFactory : ISourceRepositoryFactory
    {
        private readonly string _connectionString;
        private readonly string _database;
        private readonly string _collectionName;

        public SourceRepositoryFactory(string connectionString, string database, string collectionName)
        {
            _connectionString = connectionString;
            _database = database;
            _collectionName = collectionName;
        }
        
        public ISourceRepository Create(ILogger logger)
        {
            // TODO check DB for existence
            // TODO check collection for existence
            var collection = new MongoClient(_connectionString).GetDatabase(_database)
                .GetCollection<BsonDocument>(_collectionName);
            return new SourceRepository(collection, logger);
        }
    }
}