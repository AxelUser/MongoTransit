using MongoDB.Driver;

namespace MongoTransit.IntegrationTests
{
    public class IntegrationHelper
    {
        public const string ConnectionString = "mongodb://localhost:27117";

        public static IMongoClient CreateClient() => new MongoClient(ConnectionString);
    }
}