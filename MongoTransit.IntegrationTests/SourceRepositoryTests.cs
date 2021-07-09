using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using Mongo2Go;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Storage;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.IntegrationTests
{
    
    public class SourceRepositoryTests: IDisposable
    {
        private readonly List<MongoDbRunner> _runners = new();
        private readonly Fixture _fixture;
        private readonly IMongoCollection<BsonDocument> _sourceCollection;
        private readonly SourceRepository _sut;

        public SourceRepositoryTests()
        {
            _fixture = new Fixture();

            var (_, collection) = CreateConnection();
            _sourceCollection = collection;

            _sut = new SourceRepository(_sourceCollection, new Mock<ILogger>().Object);
        }

        #region ReadDocumentsAsync

        [Fact]
        public async Task ReadDocumentsAsync_ShouldWriteAllDocumentsIntoChannel_EmptyFilter()
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            var finderMock = new Mock<IDestinationDocumentFinder>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(_ => new BsonDocument
            {
                ["Value"] = _fixture.Create<string>(),
            }));

            // Act
            await _sut.ReadDocumentsAsync(new BsonDocument(), channel, 10, false, Array.Empty<string>(),
                false, finderMock.Object, CancellationToken.None);

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
            var finderMock = new Mock<IDestinationDocumentFinder>();
            var currentDate = DateTime.UtcNow;
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(idx => new BsonDocument
            {
                ["Modified"] = new BsonDateTime(currentDate.AddMinutes(idx))
            }));
            var filerByDate = new BsonDocument("Modified", new BsonDateTime(currentDate.AddMinutes(50)));

            // Act
            await _sut.ReadDocumentsAsync(filerByDate, channel, 10, false, Array.Empty<string>(),
                false, finderMock.Object, CancellationToken.None);
            
            // Assert
            var actual = await ReadDocumentsFromChannelAsync(channel);
            var existing = _sourceCollection.Find(filerByDate).ToList();
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
            var finderMock = new Mock<IDestinationDocumentFinder>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(_ => new BsonDocument
            {
                ["Value"] = _fixture.Create<string>(),
            }));

            // Act
            await _sut.ReadDocumentsAsync(new BsonDocument(), channel, batchSize, false, Array.Empty<string>(),
                false, finderMock.Object, CancellationToken.None);

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
                ["Value"] = _fixture.Create<string>(),
            }));
            
            // Act
            await _sut.ReadDocumentsAsync(new BsonDocument(), channel, 10, false, Array.Empty<string>(),
                false, finderMock.Object, CancellationToken.None);
            
            // Assert
            finderMock.VerifyNoOtherCalls();
        }
        
        [Fact]
        public async Task ReadDocumentsAsync_ShouldGetFilterKeysFromDestination_KeyFetchIsEnabled()
        {
            // Arrange
            var channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            await _sourceCollection.InsertManyAsync(Enumerable.Range(0, 100).Select(idx => new BsonDocument
            {
                ["Key"] = $"source_{idx}",
                ["Value"] = _fixture.Create<string>(),
            }));
            var (_, destinationCollection) = CreateConnection();
            var destValues = _sourceCollection.FindSync(new BsonDocument()).ToList().Select(document =>
            {
                document["Key"] = document["Key"].AsString.Replace("source_", "destination_");
                return document;
            });
            await destinationCollection.InsertManyAsync(destValues);

            var finder = new DestinationRepository(destinationCollection, new Mock<ILogger>().Object);

            // Act
            await _sut.ReadDocumentsAsync(new BsonDocument(), channel, 10, true, new[] { "Key" },
                false, finder, CancellationToken.None);
            
            // Assert
            var actualFilters = (await ReadReplacesFromChannelAsync(channel)).Select(model =>
                model.Filter.Render(_sourceCollection.DocumentSerializer,
                    _sourceCollection.Settings.SerializerRegistry));
            actualFilters.Should().OnlyContain(filter => filter["Key"].AsString.StartsWith("destination_"));
        }

        #endregion

        public void Dispose()
        {
            foreach (var runner in _runners)
            {
                runner.Dispose();
            }
        }

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
        
        private (MongoDbRunner runner, IMongoCollection<BsonDocument> collection) CreateConnection()
        {
            var runner = MongoDbRunner.Start();
            var client = new MongoClient(runner.ConnectionString);
            var database = client.GetDatabase("SourceRepositoryTest");
            var collection = database.GetCollection<BsonDocument>("TestCollection");
            _runners.Add(runner);
            return (runner, collection);
        }

        #endregion
    }
}