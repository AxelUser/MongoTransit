using System;
using System.Collections.Generic;
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
        // TODO: clean-up collections
        
        [Theory]
        [InlineData(1_000)]
        public async Task RunAsync_ShouldTransferAllData_DestinationCollectionIsSharded(int documentsCount)
        {
            // Arrange
            var database = Fixture.Create<string>();
            var collection = Fixture.Create<string>();
            await CreateCollectionAsync(database, collection);

            var entities = Fixture.CreateMany<Entity>(documentsCount);
            await SourceClient.GetDatabase(database).GetCollection<Entity>(collection).InsertManyAsync(entities);
            var transitOptions = new CollectionTransitOptions(SourceConnectionString, DestinationConnectionString,
                database, collection, new[] { nameof(Entity.ShardedKey) }, true, 4, 100, true, null);
            
            // Act
            await TransitRunner.RunAsync(CreateLogger(), new[] { transitOptions }, SingeCycle(), false,
                TimeSpan.FromSeconds(3), CancellationToken.None);
            
            // Assert
            var count = await DestinationClient.GetDatabase(database).GetCollection<Entity>(collection)
                .CountDocumentsAsync(FilterDefinition<Entity>.Empty);
            count.Should().Be(documentsCount);
        }

        #region additional methods

        private async Task CreateCollectionAsync(string database, string collection)
        {
            await SourceClient.GetDatabase(database).CreateCollectionAsync(collection);
            await Helper.CreateShardedCollectionAsync(database, collection, nameof(Entity.ShardedKey));
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

        #endregion
    }
}