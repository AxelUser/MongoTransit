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

namespace MongoTransit.IntegrationTests
{
    public class TransitRunnerTests: IntegrationTestBase
    {
        public record TestTransitOptions(string Database,
            string Collection,
            bool FetchKey,
            IterativeTransitOptions IterativeOptions);

        public static IEnumerable<object[]> GetTestTransitOptions =>
            new List<object[]>
            {
                new object[]
                {
                    new TestTransitOptions[]
                    {
                        new("Test", "Test", true, null)
                    }
                }
            };
        
        
        // TODO: clean-up collections
        private readonly HashSet<(string Database, string Collection)> _createdCollections = new();

        [Theory]
        [MemberData(nameof(GetTestTransitOptions))]
        public async Task RunAsync_ShouldTransferAllData_DestinationCollectionIsSharded(TestTransitOptions[] options)
        {
            // Arrange
            const int documentsCount = 1_000;

            foreach (var option in options)
            {
                await CreateCollectionAsync(option.Database, option.Collection);
                var entities = Fixture.CreateMany<Entity>(documentsCount);
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
            
            // Assert
            foreach (var option in options)
            {
                var count = await DestinationClient.GetDatabase(option.Database).GetCollection<Entity>(option.Collection)
                    .CountDocumentsAsync(FilterDefinition<Entity>.Empty);
                count.Should().Be(documentsCount,
                    $"Collection {option.Database}.{option.Collection} should have all documents");
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