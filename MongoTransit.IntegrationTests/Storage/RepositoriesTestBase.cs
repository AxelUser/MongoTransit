using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using AutoFixture;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.IntegrationTests.Helpers;

namespace MongoTransit.IntegrationTests.Storage
{
    public abstract class RepositoriesTestBase: IDisposable
    {
        private readonly List<(string database, string collection)> _createdCollections = new();
        protected readonly Fixture Fixture = new();
        private readonly MongoClient _client;

        protected RepositoriesTestBase()
        {
            _client = new MongoClient(Integration.StandaloneConnectionString);
        }
        
        protected IMongoCollection<BsonDocument> CreateConnection(string testName)
        {
            var databaseName = $"{testName}{Guid.NewGuid()}";
            var database = _client.GetDatabase(databaseName);
            var collectionName = $"{testName}{Guid.NewGuid()}";
            var collection = database.GetCollection<BsonDocument>(collectionName);
            _createdCollections.Add((databaseName, collectionName));
            return collection;
        }

        public void Dispose()
        {
            foreach (var (database, collection) in _createdCollections)
            {
                _client.GetDatabase(database).DropCollection(collection);
                if (!_client.GetDatabase(database).ListCollections().Any())
                    _client.DropDatabase(database);
            }
        }
    }
}