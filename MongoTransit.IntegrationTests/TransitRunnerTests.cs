using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using MongoDB.Driver;
using MongoTransit.IntegrationTests.Helpers;
using MongoTransit.Options;
using MongoTransit.Transit;
using Serilog;
using Serilog.Events;
using Xunit;
using Xunit.Abstractions;

namespace MongoTransit.IntegrationTests
{
    public class TransitRunnerTests: IntegrationTestBase, IDisposable
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public record TestTransitOptions(string Database,
            string Collection,
            bool FetchKey,
            IterativeTransitOptions IterativeOptions);

        #region Test Cases

        #region Full restore

        public static IEnumerable<object[]> FullRestoreTestTransitOptions =>
            new List<object[]>
            {
                new object[]
                {
                    "Full restore - Single database with multiple collections",
                    new TestTransitOptions[]
                    {
                        new("TestDb", "Test1", true, null),
                        new("TestDb", "Test2", true, null),
                        new("TestDb", "Test3", true, null)
                    }
                },
                new object[]
                {
                    "Full restore - Multiple databases with multiple collections",
                    new TestTransitOptions[]
                    {
                        new("TestDb1", "Test1", true, null),
                        new("TestDb1", "Test2", true, null),
                        new("TestDb2", "Test1", true, null),
                        new("TestDb2", "Test2", true, null),
                    }
                }
            };

        #endregion

        #region Iterative restore

        public static IEnumerable<object[]> IterativeRestoreTestTransitOptions =>
            new List<object[]>
            {
                new object[]
                {
                    "Iterative restore - Single database with multiple collections",
                    new TestTransitOptions[]
                    {
                        new("TestDb", "Test1", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null)),
                        new("TestDb", "Test2", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null)),
                        new("TestDb", "Test3", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null))
                    }
                },
                new object[]
                {
                    "Iterative restore - Multiple databases with multiple collections",
                    new TestTransitOptions[]
                    {
                        new("TestDb1", "Test1", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null)),
                        new("TestDb1", "Test2", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null)),
                        new("TestDb2", "Test1", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null)),
                        new("TestDb2", "Test2", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null)),
                    }
                },
                new object[]
                {
                    "Iterative restore - Single database with multiple collections and specified offset",
                    new TestTransitOptions[]
                    {
                        new("TestDb", "Test1", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.FromSeconds(1),  null)),
                        new("TestDb", "Test2", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.FromSeconds(10), null)),
                        new("TestDb", "Test3", true, new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.FromMinutes(5), null))
                    }
                },
            };

        #endregion
        
        #endregion

        private readonly HashSet<(string Database, string Collection)> _createdCollections = new();

        public TransitRunnerTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Theory]
        [MemberData(nameof(FullRestoreTestTransitOptions))]
        public async Task RunAsync_ShouldTransferAllData_DestinationCollectionIsSharded_FullRestore(string testCase, TestTransitOptions[] options)
        {
            // Arrange
            const int documentsCount = 1_000;
            
            _testOutputHelper.WriteLine($"Test case: {testCase}");

            foreach (var option in options)
            {
                await CreateCollectionAsync(option.Database, option.Collection);
                var entities = Fixture.CreateMany<Entity>(documentsCount).ToArray();
                await SourceClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .InsertManyAsync(entities);
            }

            var transitOptions = options.Select(o => new CollectionTransitOptions(SourceConnectionString,
                DestinationConnectionString,
                o.Database, o.Collection, new[] { nameof(Entity.ShardedKey) }, o.FetchKey, 4, 100, true,
                null)).ToArray(); 
            
            // Act
            var results = await TransitRunner.RunAsync(CreateLogger(), transitOptions, SingeCycle(), false,
                TimeSpan.FromSeconds(3), CancellationToken.None);
            
            // Assert
            results.Processed.Should().Be(documentsCount * options.Length);
            foreach (var option in options)
            {
                var sourceEntities = await SourceClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .Find(FilterDefinition<Entity>.Empty).ToListAsync();
                var destinationEntities = await DestinationClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .Find(FilterDefinition<Entity>.Empty).ToListAsync();
                destinationEntities.Should().BeEquivalentTo(sourceEntities,
                    $"Destination collection {option.Database}.{option.Collection} should have all documents from source");
            }
        }
        
        [Theory]
        [MemberData(nameof(IterativeRestoreTestTransitOptions))]
        public async Task RunAsync_ShouldTransferAllData_DestinationCollectionIsSharded_IterativeRestore(string testCase, TestTransitOptions[] options)
        {
            // Arrange
            const int firstLoadCount = 500;
            
            _testOutputHelper.WriteLine($"Test case: {testCase}");
            var startDateForFirstLoad = Fixture.Create<DateTime>();

            foreach (var option in options)
            {
                await CreateCollectionAsync(option.Database, option.Collection);
                var entities = Enumerable
                    .Range(0, firstLoadCount)
                    .Select(i => Fixture.Build<Entity>()
                        .With(e => e.Modified, startDateForFirstLoad.AddSeconds(i))
                        .Create())
                    .ToArray(); 
                await SourceClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .InsertManyAsync(entities);
            }

            var transitOptions = options.Select(o => new CollectionTransitOptions(SourceConnectionString,
                DestinationConnectionString,
                o.Database, o.Collection, new[] { nameof(Entity.ShardedKey) }, o.FetchKey, 4, 100, true,
                o.IterativeOptions)).ToArray(); 
            
            // Act
            await TransitRunner.RunAsync(CreateLogger(), transitOptions, SingeCycle(), false,
                TimeSpan.FromSeconds(3), CancellationToken.None);
            
            const int secondLoadCount = 500;
            var startDateForSecondLoad = startDateForFirstLoad.AddSeconds(firstLoadCount);
            foreach (var option in options)
            {
                var entities = Enumerable
                    .Range(0, secondLoadCount)
                    .Select(i => Fixture.Build<Entity>()
                        .With(e => e.Modified, startDateForSecondLoad.AddSeconds(i) - option.IterativeOptions.Offset)
                        .Create())
                    .ToArray(); 
                await SourceClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .InsertManyAsync(entities);
            }
            
            var secondRunResults = await TransitRunner.RunAsync(CreateLogger(), transitOptions, SingeCycle(), false,
                TimeSpan.FromSeconds(3), CancellationToken.None);
            
            // Assert
            secondRunResults.Processed.Should()
                .BeGreaterThan(secondLoadCount * options.Length).And
                .BeLessThan((firstLoadCount + secondLoadCount) * options.Length);
            foreach (var option in options)
            {
                var sourceEntities = await SourceClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .Find(FilterDefinition<Entity>.Empty).ToListAsync();
                var destinationEntities = await DestinationClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .Find(FilterDefinition<Entity>.Empty).ToListAsync();
                destinationEntities.Should().BeEquivalentTo(sourceEntities,
                    $"Destination collection {option.Database}.{option.Collection} should have all documents from source");
            }
        }
        
        [Fact(Skip = "Unstable: seems like data in source is not updated before transferred second time, so no retries")]
        public async Task RunAsync_ShouldTransferAllDataWithRetries_DestinationCollectionIsSharded_IterativeRestore_ShardedKeyChangedInSource()
        {
            // Arrange
            var transitOption = new CollectionTransitOptions(SourceConnectionString,
                DestinationConnectionString,
                "TestDb", "TestC", new[] { nameof(Entity.ShardedKey) }, true, 4, 100, true,
                new IterativeTransitOptions(nameof(Entity.Modified), TimeSpan.Zero, null));
            
            var zonesRanges = new ZoneRange[]
            {
                new("ZoneA", 0, 100),
                new("ZoneB", 100, 200)
            };
            var shards = await DestinationHelper.ListShardsAsync();
            await DestinationHelper.AddShardToZoneAsync(shards[0].Id, zonesRanges[0].Zone);
            await DestinationHelper.AddShardToZoneAsync(shards[1].Id, zonesRanges[1].Zone);

            var (sourceCollection, destinationCollection) = await CreateCollectionAsync(transitOption.Database, transitOption.Collection);

            foreach (var range in zonesRanges)
            {
                await DestinationHelper.UpdateZoneKeyRangeAsync(transitOption.Database, transitOption.Collection,
                    nameof(Entity.ShardedKey), range);
            }

            var originalModified = Fixture.Create<DateTime>();
            var originalEntities = Enumerable.Range(0, 100)
                .Select(keyValue => Fixture.Build<Entity>()
                    .With(e => e.ShardedKey, keyValue) // Should be located at shard for ZoneA
                    .With(e => e.Modified, originalModified)
                    .Create())
                .ToArray();
            
            await sourceCollection.InsertManyAsync(originalEntities);

            var resultsBefore = await TransitRunner.RunAsync(CreateLogger(), new [] {transitOption}, SingeCycle(), false,
                TimeSpan.FromSeconds(3), CancellationToken.None);
            
            // Wait until all data is acknowledged
            await Task.Delay(TimeSpan.FromSeconds(10));

            await sourceCollection.UpdateManyAsync(FilterDefinition<Entity>.Empty,
                new UpdateDefinitionBuilder<Entity>()
                    .Inc(e => e.ShardedKey, 100) // Should be transferred at shard for ZoneB
                    .Set(e => e.Modified, originalModified.AddHours(1)));

            // Act
            var resultsAfter = await TransitRunner.RunAsync(CreateLogger(), new [] {transitOption}, SingeCycle(), false,
                TimeSpan.FromSeconds(3), CancellationToken.None);
            
            // Wait until all data is acknowledged
            await Task.Delay(TimeSpan.FromSeconds(20));
            
            // Assert
            resultsBefore.Processed.Should().Be(originalEntities.Length);
            resultsAfter.Retried.Should().Be(originalEntities.Length);
            
            var sourceEntities = await sourceCollection.Find(FilterDefinition<Entity>.Empty).ToListAsync();
            var destinationEntities = await destinationCollection.Find(FilterDefinition<Entity>.Empty).ToListAsync();
            destinationEntities.Should().BeEquivalentTo(sourceEntities,
                $"Destination collection {transitOption.Database}.{transitOption.Collection} should have all documents from source");
        }

        #region additional methods

        private async Task<(IMongoCollection<Entity> sourceCollection, IMongoCollection<Entity> destinationCollection)> CreateCollectionAsync(string database, string collection)
        {
            await SourceClient.GetDatabase(database).CreateCollectionAsync(collection);
            await DestinationHelper.CreateShardedCollectionAsync(database, collection, nameof(Entity.ShardedKey));
            _createdCollections.Add((database, collection));

            var source = SourceClient.GetDatabase(database).GetCollection<Entity>(collection);
            var dest = DestinationClient.GetDatabase(database).GetCollection<Entity>(collection);

            return (source, dest);
        }

        private static ILogger CreateLogger()
        {
            return new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.WithProperty("Scope", "Runner")
                .WriteTo.Console(LogEventLevel.Debug,
                    "[{Timestamp:HH:mm:ss} {Level:u3}][{Scope}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger(); 
        }

        private static IEnumerable<int> SingeCycle()
        {
            yield return 0;
        }

        public void Dispose()
        {
            foreach (var (database, collection) in _createdCollections)
            {
                SourceClient.GetDatabase(database).DropCollection(collection);
                if (!SourceClient.GetDatabase(database).ListCollections().Any())
                    SourceClient.DropDatabase(database);
                
                DestinationClient.GetDatabase(database).DropCollection(collection);
                if (!DestinationClient.GetDatabase(database).ListCollections().Any())
                    DestinationClient.DropDatabase(database);
            }
        }

        #endregion
    }
}