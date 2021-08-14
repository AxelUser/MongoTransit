using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using MongoDB.Driver;
using MongoTransit.Options;
using MongoTransit.Transit;
using Serilog;
using Serilog.Events;
using Xunit;
using Xunit.Abstractions;

namespace MongoTransit.IntegrationTests
{
    public class TransitRunnerTests: IntegrationTestBase
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

        #region additional methods

        private async Task CreateCollectionAsync(string database, string collection)
        {
            await SourceClient.GetDatabase(database).CreateCollectionAsync(collection);
            await Helper.CreateShardedCollectionAsync(database, collection, nameof(Entity.ShardedKey));
            _createdCollections.Add((database, collection));
        }

        private ILogger CreateLogger()
        {
            return new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.WithProperty("Scope", "Runner")
                .WriteTo.Console(LogEventLevel.Debug,
                    "[{Timestamp:HH:mm:ss} {Level:u3}][{Scope}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger(); 
        }

        private IEnumerable<int> SingeCycle()
        {
            yield return 0;
        }

        public override void Dispose()
        {
            foreach (var (database, collection) in _createdCollections)
            {
                DestinationClient.GetDatabase(database).DropCollection(collection);
                if (!DestinationClient.GetDatabase(database).ListCollections().Any())
                    DestinationClient.DropDatabase(database);
            }
            
            base.Dispose();
        }

        #endregion
    }
}