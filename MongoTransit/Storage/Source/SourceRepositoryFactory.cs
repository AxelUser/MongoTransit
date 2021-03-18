using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Storage.Source
{
    public class SourceRepositoryFactory : ISourceRepositoryFactory
    {
        private readonly IMongoCollection<BsonDocument> _collection;

        public SourceRepositoryFactory(string connectionString, string database, string collectionName)
        {
            // TODO check DB for existence
            // TODO check collection for existence
            _collection = new MongoClient(connectionString).GetDatabase(database)
                .GetCollection<BsonDocument>(collectionName);
        }
        
        public ISourceRepository Create(ILogger logger)
        {
            return new SourceRepository(_collection, logger);
        }
    }
}