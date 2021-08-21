using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Storage;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using MongoTransit.Storage.Source.Models;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.IntegrationTests.Storage
{
    
    public class SourceRepositoryTests: RepositoriesTestBase
    {
        private readonly IMongoCollection<BsonDocument> _sourceCollection;
        private readonly SourceRepository _sut;

        public SourceRepositoryTests()
        {
            var (_, collection) = CreateConnection(nameof(SourceRepositoryTests));
            _sourceCollection = collection;

            _sut = new SourceRepository(_sourceCollection, new Mock<ILogger>().Object);
        }

        #region ReadDocumentsAsync

        [Fact]
        public async Task ReadDocumentsAsync_ShouldWriteAllDocumentsIntoChannel_EmptyFilter()
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            }));

            // Act
            await _sut.ReadDocumentsAsync(SourceFilter.Empty, channel, 10, Array.Empty<string>(),
                false, null, CancellationToken.None);

            // Assert
            var actual = await ReadDocumentsFromChannelAsync(channel);
            var existing = _sourceCollection.Find(new BsonDocument()).ToList();
            actual.Should().BeEquivalentTo(existing);
        }

        [Fact]
        public async Task ReadDocumentsAsync_ShouldWriteAllDocumentsIntoChannel_FilterByDate()
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            var currentDate = DateTime.UtcNow;
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(idx => new BsonDocument
            {
                ["Modified"] = new BsonDateTime(currentDate.AddMinutes(idx))
            }));
            var filerByDate = new SourceFilter("Modified", currentDate.AddMinutes(50));

            // Act
            await _sut.ReadDocumentsAsync(filerByDate, channel, 10, Array.Empty<string>(),
                false, null, CancellationToken.None);
            
            // Assert
            var actual = await ReadDocumentsFromChannelAsync(channel);
            var existing = _sourceCollection.Find(filerByDate.ToBsonDocument()).ToList();
            actual.Should().BeEquivalentTo(existing);
        }
        
        [Theory]
        [InlineData(1, 100)]
        [InlineData(10, 10)]
        [InlineData(100, 1)]
        public async Task ReadDocumentsAsync_ShouldWriteIntoBatchesOfDefinedSize_BatchSizeProvided(int batchSize, int expectedBatches)
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            }));

            // Act
            await _sut.ReadDocumentsAsync(SourceFilter.Empty, channel, batchSize, Array.Empty<string>(),
                false, null, CancellationToken.None);

            // Assert
            var actual = await ReadReplaceBatchesFromChannelAsync(channel);
            actual.Should()
                .HaveCount(expectedBatches) 
                .And.OnlyContain(batch => batch.Count <= batchSize)
                .And.NotContain(batch => !batch.Any());
        }
        
        [Fact]
        public async Task ReadDocumentsAsync_ShouldNotFetchDocumentsFromDestination_KeyFetchIsDisabled()
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            var finderMock = new Mock<IDestinationDocumentFinder>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            }));
            
            // Act
            await _sut.ReadDocumentsAsync(SourceFilter.Empty, channel, 10, Array.Empty<string>(),
                false, null, CancellationToken.None);
            
            // Assert
            finderMock.VerifyNoOtherCalls();
        }
        
        [Fact]
        public async Task ReadDocumentsAsync_ShouldGetFilterKeysFromDestinationAndReplacementsFromSource_KeyFetchIsEnabled()
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(idx => new BsonDocument
            {
                ["Key"] = $"source_{idx}",
                ["Value"] = $"source_{Fixture.Create<string>()}",
            }));
            var (_, destinationCollection) = CreateConnection();
            var destValues = _sourceCollection.FindSync(new BsonDocument()).ToList().Select(document =>
            {
                document["Key"] = document["Key"].AsString.Replace("source_", "destination_");
                document["Value"] = document["Value"].AsString.Replace("source_", "destination_");
                return document;
            });
            await destinationCollection.InsertManyAsync(destValues);

            var finder = new DestinationRepository(destinationCollection, new Mock<ILogger>().Object);

            // Act
            await _sut.ReadDocumentsAsync(SourceFilter.Empty, channel, 10, new[] { "Key" },
                false, finder, CancellationToken.None);
            
            // Assert
            var actualModels = await ReadReplacesFromChannelAsync(channel);
            var actualReplaceFilters = actualModels.Select(model =>
                model.Filter.Render(_sourceCollection.DocumentSerializer,
                    _sourceCollection.Settings.SerializerRegistry));
            var actualReplacements = actualModels.Select(model => model.Replacement);
            actualReplaceFilters.Should().OnlyContain(filter =>
                filter["Key"].AsString.StartsWith("destination_") && filter.Names.Count() == 1);
            actualReplacements.Should().OnlyContain(r =>
                r["Key"].AsString.StartsWith("source_") && r["Value"].AsString.StartsWith("source_"));
        }
        
        [Fact]
        public async Task ReadDocumentsAsync_ShouldGetFilterWithId_EmptyKeys()
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            var finderMock = new Mock<IDestinationDocumentFinder>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            }));

            // Act
            await _sut.ReadDocumentsAsync(SourceFilter.Empty, channel, 10, Array.Empty<string>(),
                false, null, CancellationToken.None);

            // Assert
            var actualFilters = (await ReadReplacesFromChannelAsync(channel)).Select(model =>
                model.Filter.Render(_sourceCollection.DocumentSerializer,
                    _sourceCollection.Settings.SerializerRegistry));
            actualFilters.Should().OnlyContain(filter => filter.Names.First() == "_id" && filter.Names.Count() == 1);
        }

        #endregion

        #region CountAllAsync

        [Fact]
        public async Task CountAllAsync_ShouldReturnZero_EmptyCollection()
        {
            // Act
            var actual = await _sut.CountAllAsync(CancellationToken.None);

            // Assert
            actual.Should().Be(0);
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task CountAllAsync_ShouldReturnNumberOfDocumentsInCollection_CollectionIsNotEmpty(int documents)
        {
            // Arrange
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, documents).Select(_ => new BsonDocument
            {
                ["Value"] = Fixture.Create<string>(),
            }));
            
            // Act
            var actual = await _sut.CountAllAsync(CancellationToken.None);

            // Assert
            actual.Should().Be(documents);
        }

        #endregion
        
        #region CountLagAsync

        [Fact]
        public async Task CountLagAsync_ShouldReturnZero_EmptyCollection()
        {
            // Act
            var actual = await _sut.CountLagAsync(new SourceFilter("Modified", DateTime.UtcNow), CancellationToken.None);

            // Assert
            actual.Should().Be(0);
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task CountLagAsync_ShouldReturnAtLeastOverlappingCount_CheckpointValueIsEqualsToLastKnownFieldValue(int elementsWithMaxCheckpointFieldValue)
        {
            // Arrange
            var lastCheckpoint = DateTime.UtcNow;
            await _sourceCollection.InsertOneAsync(new BsonDocument
            {
                ["Modified"] = new BsonDateTime(lastCheckpoint.AddMinutes(-1))
            });

            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, elementsWithMaxCheckpointFieldValue)
                .Select(_ => new BsonDocument
                {
                    ["Modified"] = new BsonDateTime(lastCheckpoint)
                }));
            
            // Act
            var actual = await _sut.CountLagAsync(new SourceFilter("Modified", lastCheckpoint), CancellationToken.None);

            // Assert
            actual.Should().Be(elementsWithMaxCheckpointFieldValue);
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task CountLagAsync_ShouldReturnNumberOfDocumentsGteThanCheckpoint_CheckpointValueIsLessThanMaximumAvailable(int documentsAfterCheckpoint)
        {
            // Arrange
            var lastCheckpoint = DateTime.UtcNow;
            await _sourceCollection.InsertManyAsync(new []
            {
                new BsonDocument
                {
                    ["Modified"] = new BsonDateTime(lastCheckpoint.AddMinutes(-1))
                },
                new BsonDocument
                {
                    ["Modified"] = new BsonDateTime(lastCheckpoint)
                },
            });
            await _sourceCollection.InsertManyAsync(Enumerable.Range(1, documentsAfterCheckpoint).Select(
                minutesOffset => new BsonDocument
                {
                    ["Modified"] = new BsonDateTime(lastCheckpoint.AddMinutes(minutesOffset))
                }));

            // Act
            var actual = await _sut.CountLagAsync(new SourceFilter("Modified", lastCheckpoint), CancellationToken.None);

            // Assert
            actual.Should().Be(documentsAfterCheckpoint + 1);
        }

        #endregion

        #region helpers

        private static async Task<List<BsonDocument>> ReadDocumentsFromChannelAsync(ChannelReader<List<ReplaceOneModel<BsonDocument>>> reader)
        {
            var actual = new List<BsonDocument>();
            await foreach (var batch in reader.ReadAllAsync())
                actual.AddRange(batch.Select(m => m.Replacement));
            return actual;
        }
        
        private static async Task<List<List<ReplaceOneModel<BsonDocument>>>> ReadReplaceBatchesFromChannelAsync(ChannelReader<List<ReplaceOneModel<BsonDocument>>> reader)
        {
            var actual = new List<List<ReplaceOneModel<BsonDocument>>>();
            await foreach (var batch in reader.ReadAllAsync())
                actual.Add(batch);
            return actual;
        }
        
        private static async Task<List<ReplaceOneModel<BsonDocument>>> ReadReplacesFromChannelAsync(ChannelReader<List<ReplaceOneModel<BsonDocument>>> reader)
        {
            var actual = new List<ReplaceOneModel<BsonDocument>>();
            await foreach (var batch in reader.ReadAllAsync())
                actual.AddRange(batch);
            return actual;
        }
        
        #endregion
    }
}