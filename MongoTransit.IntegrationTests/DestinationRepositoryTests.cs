using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.IntegrationTests.Extensions;
using MongoTransit.Storage.Destination;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.IntegrationTests
{
    public class DestinationRepositoryTests: RepositoriesTestBase
    {
        
        private readonly IMongoCollection<BsonDocument> _destCollection;
        private readonly DestinationRepository _sut;
        
        public DestinationRepositoryTests()
        {
            var (_, collection) = CreateConnection(nameof(DestinationRepositoryTests));
            _destCollection = collection;

            _sut = new DestinationRepository(_destCollection, new Mock<ILogger>().Object);
        }

        #region FindLastCheckpointAsync

        [Fact]
        public async Task FindLastCheckpointAsync_ShouldReturnNull_EmptyCollection()
        {
            // Act
            var actual = await _sut.FindLastCheckpointAsync("Modified", CancellationToken.None);
            
            // Assert
            actual.Should().BeNull();
        }
        
        [Fact]
        public async Task FindLastCheckpointAsync_ShouldReturnNull_NoCheckpointValueInCollection()
        {
            // Arrange
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["Value"] = Fixture.Create<string>()
                }));
            
            // Act
            var actual = await _sut.FindLastCheckpointAsync("Modified", CancellationToken.None);
            
            // Assert
            actual.Should().BeNull();
        }
        
        [Fact]
        public async Task FindLastCheckpointAsync_ShouldReturnMaxCheckpointValue_HasCheckpointValuesInCollection()
        {
            // Arrange
            // Truncate checkpoint value due to differences in MongoDb and .Net resolution of DateTime, so
            // assertion will be easier
            var lastCheckpoint = DateTime.UtcNow.Truncate(TimeSpan.FromSeconds(1));
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(offset => new BsonDocument
                {
                    ["Modified"] = new BsonDateTime(lastCheckpoint.AddMinutes(-offset))
                }));
            
            // Act
            var actual = await _sut.FindLastCheckpointAsync("Modified", CancellationToken.None);
            
            // Assert
            actual.Should().Be(lastCheckpoint.AddMilliseconds(-lastCheckpoint.Millisecond));
        }

        #endregion
    }
}