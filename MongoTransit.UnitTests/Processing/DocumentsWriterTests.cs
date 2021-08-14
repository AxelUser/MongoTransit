﻿using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Processing;
using MongoTransit.Processing.Workers;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.UnitTests.Processing
{
    public class DocumentsWriterTests
    {
        private DocumentsWriter _writer;
        private readonly Fixture _fixture;
        private readonly Mock<ILogger> _loggerMock;
        private readonly Mock<IWriteWorkerFactory> _workerFactoryMock;

        public DocumentsWriterTests()
        {
            _fixture = new Fixture();

            _loggerMock = new Mock<ILogger>();
            _workerFactoryMock = new Mock<IWriteWorkerFactory>();

            _writer = new DocumentsWriter(4, 4, _fixture.Create<string>(), _workerFactoryMock.Object, _loggerMock.Object);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task WriteAsync_ShouldStartExactNumberOfInsertionWorkers_NumberOfInsertionWorkersGreaterThanZero(int numberOfWorkers)
        {
            // Arrange
            _workerFactoryMock.Setup(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WorkerResult(100, 0, 0));
            _workerFactoryMock.Setup(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WorkerResult(100, 0, 0));
            _writer = new DocumentsWriter(numberOfWorkers, 0, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            await _writer.WriteAsync(CancellationToken.None);
            
            // Assert
            _workerFactoryMock.Verify(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                It.IsAny<ILogger>(), It.IsAny<CancellationToken>()), Times.Exactly(numberOfWorkers));
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task WriteAsync_ShouldStartExactNumberOfRetryWorkers_NumberOfInsertionAndRetryWorkersGreaterThanZero(int numberOfWorkers)
        {
            // Arrange
            _workerFactoryMock.Setup(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WorkerResult(100L, 0L, 0L));
            _workerFactoryMock.Setup(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WorkerResult(100L, 0L, 0L));
            _writer = new DocumentsWriter(numberOfWorkers, numberOfWorkers, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            await _writer.WriteAsync(CancellationToken.None);
            
            // Assert
            _workerFactoryMock.Verify(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                It.IsAny<ILogger>(), It.IsAny<CancellationToken>()), Times.Exactly(numberOfWorkers));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task WriteAsync_ShouldReturnSumOfWorkersResults_InsertionAndRetryWorkersAreStarted(int insertionWorkers)
        {
            // Arrange
            var result = new WorkerResult(3L, 2L, 1L);
            _workerFactoryMock.Setup(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(result);
            _workerFactoryMock.Setup(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(result);
            
            _writer = new DocumentsWriter(insertionWorkers, insertionWorkers, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            var actual = await _writer.WriteAsync(CancellationToken.None);
            
            // Assert
            var expected = new TransferResults(result.Successful * insertionWorkers * 2, result.Retryable * insertionWorkers * 2,
                result.Failed * insertionWorkers * 2);
            actual.Should().Be(expected);
        }
    }
}