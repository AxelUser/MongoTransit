using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransit.IntegrationTests.Helpers
{
    public class MongoHelper
    {
        private readonly IMongoClient _client;
        private readonly IMongoDatabase _adminDb;

        public MongoHelper(IMongoClient client)
        {
            _client = client;
            _adminDb = _client.GetDatabase("admin");
        }

        public async Task CreateShardedCollectionAsync(string databaseName, string collectionName, string key)
        {
            await _client.GetDatabase(databaseName).CreateCollectionAsync(collectionName);
            VerifyOk(await _adminDb.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["enableSharding"] = databaseName
            }), "Enable Database Sharding");

            VerifyOk(await _adminDb.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["shardCollection"] = $"{databaseName}.{collectionName}",
                ["key"] = new BsonDocument(key, 1)
            }), "Sharding Collection");
        }

        private void VerifyOk(BsonDocument operationResult, string operationName)
        {
            var ok = operationResult["ok"].AsDouble;
            if (ok != 1.0D) throw new Exception($"{operationName} failed");
        }
    }
}