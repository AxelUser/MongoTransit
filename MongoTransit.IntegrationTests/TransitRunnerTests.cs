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

        public static IEnumerable<object[]> GetFullRestoreTestTransitOptions =>
            new List<object[]>
            {
                new object[]
                {
                    "Single database with multiple collections (Full restore)",
                    new TestTransitOptions[]
                    {
                        new("TestDb", "Test1", true, null),
                        new("TestDb", "Test2", true, null),
                        new("TestDb", "Test3", true, null)
                    }
                },
                new object[]
                {
                    "Multiple databases with multiple collections (Full restore)",
                    new TestTransitOptions[]
                    {
                        new("TestDb1", "Test1", true, null),
                        new("TestDb1", "Test2", true, null),
                        new("TestDb2", "Test1", true, null),
                        new("TestDb2", "Test2", true, null),
                    }
                }
            };
        
        
        private readonly HashSet<(string Database, string Collection)> _createdCollections = new();

        public TransitRunnerTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Theory]
        [MemberData(nameof(GetFullRestoreTestTransitOptions))]
        public async Task RunAsync_ShouldTransferAllData_DestinationCollectionIsSharded_FullRestore(string testCase, TestTransitOptions[] options)
        {
            // Arrange
            const int documentsCount = 1_000;
            
            _testOutputHelper.WriteLine($"Test case: {testCase}");
            var dbNames = new Dictionary<string, string>();

            foreach (var option in options)
            {
                dbNames.TryAdd(option.Database, $"{option.Database}-{Guid.NewGuid()}");
                var db = dbNames[option.Database];
                
                await CreateCollectionAsync(db, option.Collection);
                var entities = Fixture.CreateMany<Entity>(documentsCount);
                await SourceClient.GetDatabase(db).GetCollection<Entity>(option.Collection)
                    .InsertManyAsync(entities);
            }

            var transitOptions = options.Select(o => new CollectionTransitOptions(SourceConnectionString,
                DestinationConnectionString,
                dbNames[o.Database], o.Collection, new[] { nameof(Entity.ShardedKey) }, o.FetchKey, 4, 100, true,
                null)).ToArray(); 
            
            // Act
            await TransitRunner.RunAsync(CreateLogger(), transitOptions, SingeCycle(), false,
                TimeSpan.FromSeconds(3), CancellationToken.None);
            
            // Assert
            foreach (var option in options)
            {
                var db = dbNames[option.Database];
                var count = await DestinationClient.GetDatabase(db).GetCollection<Entity>(option.Collection)
                    .CountDocumentsAsync(FilterDefinition<Entity>.Empty);
                count.Should().Be(documentsCount,
                    $"Collection {db}.{option.Collection} should have all documents");
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