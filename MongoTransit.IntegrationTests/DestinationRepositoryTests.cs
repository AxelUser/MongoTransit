using System;
using System.Collections.Generic;
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

        #region ReplaceManyAsync

        [Fact]
        public async Task ReplaceManyAsync_ShouldNotDoAnyInsertions_EmptyBulk()
        {
            
            // Act
            await _sut.ReplaceManyAsync( new List<ReplaceOneModel<BsonDocument>>(), CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEmpty();
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldNotDoAnyInsertions_NullValueForBulk()
        {
            
            // Act
            await _sut.ReplaceManyAsync( null, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEmpty();
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldPerformReplacements_ReplaceById()
        {
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["Value"] = Fixture.Create<string>()
                }));
            var replacements = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList()
                .Select(document =>
                {
                    document["Value"] = Fixture.Create<string>();
                    return document;
                }).ToList();
            var replaceModels = replacements
                .Select(document =>
                    new ReplaceOneModel<BsonDocument>(new BsonDocument("_id", document["_id"]), document))
                .ToList();
            
            // Act
            await _sut.ReplaceManyAsync( replaceModels, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEquivalentTo(replacements);
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldPerformReplacements_ReplaceByField()
        {
            await _destCollection.InsertManyAsync(Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["Key"] = Fixture.Create<string>(),
                    ["Value"] = Fixture.Create<string>(),
                }));
            var replacements = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList()
                .Select(document =>
                {
                    document["Value"] = Fixture.Create<string>();
                    return document;
                }).ToList();
            var replaceModels = replacements
                .Select(document =>
                    new ReplaceOneModel<BsonDocument>(new BsonDocument("Key", document["Key"]), document))
                .ToList();
            
            // Act
            await _sut.ReplaceManyAsync( replaceModels, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEquivalentTo(replacements);
        }
        
        [Fact]
        public async Task ReplaceManyAsync_ShouldPerformInsertions_ModelHasUpsertOption()
        {
            var newDocuments = Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = Fixture.Create<Guid>(),
                    ["Value"] = Fixture.Create<string>(),
                }).ToList();
            var replaceModels = newDocuments
                .Select(document =>
                    new ReplaceOneModel<BsonDocument>(new BsonDocument("_id", document["_id"]), document)
                    {
                        IsUpsert = true
                    })
                .ToList();
            
            // Act
            await _sut.ReplaceManyAsync( replaceModels, CancellationToken.None);
            
            // Assert
            var actual = _destCollection.FindSync(FilterDefinition<BsonDocument>.Empty).ToList();
            actual.Should().BeEquivalentTo(newDocuments);
        }

        #endregion
    }
}