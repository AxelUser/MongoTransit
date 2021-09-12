using AutoFixture;
using MongoDB.Driver;
using MongoTransit.IntegrationTests.Helpers;

namespace MongoTransit.IntegrationTests
{
    public abstract class IntegrationTestBase
    {
        protected readonly Fixture Fixture = new();
        protected readonly string SourceConnectionString;
        protected readonly string DestinationConnectionString;
        protected readonly IMongoClient SourceClient;
        protected readonly IMongoClient DestinationClient;
        protected readonly MongoHelper DestinationHelper;

        protected IntegrationTestBase()
        {
            SourceConnectionString = Integration.StandaloneConnectionString;
            SourceClient = new MongoClient(Integration.StandaloneConnectionString);
            DestinationConnectionString = Integration.ShardedClusterConnectionString;
            DestinationClient = new MongoClient(Integration.ShardedClusterConnectionString);
            DestinationHelper = new MongoHelper(DestinationClient);
        }
    }
}