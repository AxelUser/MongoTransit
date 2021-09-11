using MongoDB.Driver;

namespace MongoTransit.IntegrationTests.Helpers
{
    public static class IntegrationHelper
    {
        public const string ConnectionString = "mongodb://localhost:27117";

        public static IMongoClient CreateClient() => new MongoClient(ConnectionString);
    }
}