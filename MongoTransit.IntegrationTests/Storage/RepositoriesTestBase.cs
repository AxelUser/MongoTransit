using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using AutoFixture;
using Mongo2Go;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransit.IntegrationTests.Storage
{
    public abstract class RepositoriesTestBase: IDisposable
    {
        private readonly List<MongoDbRunner> _runners = new();
        protected readonly Fixture Fixture = new Fixture();
        
        protected (MongoDbRunner runner, IMongoCollection<BsonDocument> collection) CreateConnection([CallerMemberName] string testName = "TestCollection")
        {
            var runner = MongoDbRunner.Start(additionalMongodArguments: "--quiet");
            var client = new MongoClient(runner.ConnectionString);
            var database = client.GetDatabase("TestDatabase");
            var collection = database.GetCollection<BsonDocument>(testName);
            _runners.Add(runner);
            return (runner, collection);
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