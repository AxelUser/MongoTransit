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
            }), $"Enable database '{databaseName}' sharding");

            VerifyOk(await _adminDb.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["shardCollection"] = $"{databaseName}.{collectionName}",
                ["key"] = new BsonDocument(key, 1)
            }), $"Sharding {databaseName}.{collectionName} collection by key {key}");
        }

        public async Task AddShardToZoneAsync(string shardName, string zoneName)
        {
            VerifyOk(await _adminDb.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["addShardToZone"] = shardName,
                ["zone"] = zoneName
            }), $"Adding shard {shardName} to zone {zoneName}");
        }

        public async Task UpdateZoneKeyRangeAsync(string databaseName,
            string collectionName,
            string key,
            string min,
            string max,
            string zoneName)
        {
            VerifyOk(await _adminDb.RunCommandAsync<BsonDocument>(new BsonDocument
                {
                    ["updateZoneKeyRange"] = $"{databaseName}.{collectionName}",
                    ["min"] = new BsonDocument(key, min),
                    ["max"] = new BsonDocument(key, max),
                    ["zone"] = zoneName
                }),
                $"Updating zone {zoneName} of collection {databaseName}.{collectionName} with key {key} ranges from {min} to {max}");
        }

        private void VerifyOk(BsonDocument operationResult, string operationName)
        {
            var ok = operationResult["ok"].AsDouble;
            if (ok != 1.0D) throw new Exception($"{operationName} failed");
        }
    }
}