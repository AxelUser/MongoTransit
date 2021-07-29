using System;
using System.Collections.Generic;
using AutoFixture;
using Mongo2Go;
using MongoDB.Driver;
using MongoTransit.IntegrationTests.Helpers;

namespace MongoTransit.IntegrationTests
{
    public abstract class IntegrationTestBase: IDisposable
    {
        private readonly List<MongoDbRunner> _runners = new();
        protected readonly Fixture Fixture = new Fixture();
        protected readonly string SourceConnectionString;
        protected readonly string DestinationConnectionString;
        protected readonly IMongoClient SourceClient;
        protected readonly IMongoClient DestinationClient;
        protected readonly MongoHelper Helper;

        public IntegrationTestBase()
        {
            var runner = MongoDbRunner.Start(additionalMongodArguments: "--quiet");
            _runners.Add(runner);
            SourceConnectionString = runner.ConnectionString;
            SourceClient = new MongoClient(SourceConnectionString);
            DestinationConnectionString = IntegrationHelper.ConnectionString;
            DestinationClient = new MongoClient(DestinationConnectionString);
            Helper = new MongoHelper(DestinationClient);
        }

        public void Dispose()
        {
            foreach (var runner in _runners)
            {
                runner.Dispose();
            }
        }
    }
}