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

        protected readonly Fixture Fixture = new();
        protected readonly string SourceConnectionString;
        protected readonly string DestinationConnectionString;
        protected readonly IMongoClient SourceClient;
        protected readonly IMongoClient DestinationClient;
        protected readonly MongoHelper DestinationHelper;

        protected IntegrationTestBase()
        {
            var runner = MongoDbRunner.Start(additionalMongodArguments: "--quiet");
            _runners.Add(runner);
            SourceConnectionString = runner.ConnectionString;
            SourceClient = new MongoClient(SourceConnectionString);
            DestinationConnectionString = IntegrationHelper.ConnectionString;
            DestinationClient = new MongoClient(DestinationConnectionString);
            DestinationHelper = new MongoHelper(DestinationClient);
        }

        public virtual void Dispose()
        {
            foreach (var runner in _runners)
            {
                runner.Dispose();
            }
        }
    }
}