using AutoFixture;
using MongoDB.Driver;
using MongoTransit.IntegrationTests.Helpers;

namespace MongoTransit.IntegrationTests
{
    public abstract class IntegrationTestBase
    {
        protected static readonly Fixture Fixture = new();
        protected const string SourceConnectionString = Integration.StandaloneConnectionString;
        protected const string DestinationConnectionString = Integration.ShardedClusterConnectionString;
        protected static readonly IMongoClient SourceClient = new MongoClient(Integration.StandaloneConnectionString);

        protected static readonly IMongoClient DestinationClient =
            new MongoClient(Integration.ShardedClusterConnectionString);

        protected static readonly MongoHelper DestinationHelper = new(DestinationClient);
    }
}