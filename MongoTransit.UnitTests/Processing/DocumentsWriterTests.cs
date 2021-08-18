using System;
using System.Threading;
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
        private readonly Fixture _fixture;
        private readonly Mock<ILogger> _loggerMock;
        private readonly Mock<IWriteWorkerFactory> _workerFactoryMock;

        public DocumentsWriterTests()
        {
            _fixture = new Fixture();

            _loggerMock = new Mock<ILogger>();
            _workerFactoryMock = new Mock<IWriteWorkerFactory>();
        }

        [Fact]
        public void Ctor_ShouldThrowArgumentOutOfRangeException_NumberOfInsertionWorkersLessThanZero()
        {
            // Act
            Action act = () => new DocumentsWriter(-1, 1, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);

            act.Should().ThrowExactly<ArgumentOutOfRangeException>().Which.ParamName.Should()
                .Be("insertionWorkersCount");
        }
        
        [Fact]
        public void Ctor_ShouldThrowArgumentOutOfRangeException_NumberOfRetryWorkersLessThanZero()
        {
            // Act
            Action act = () => new DocumentsWriter(1, -1, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);

            act.Should().ThrowExactly<ArgumentOutOfRangeException>().Which.ParamName.Should()
                .Be("retryWorkersCount");
        }

        [Theory]
        [InlineData(2, 2)]
        [InlineData(5, 10)]
        [InlineData(10, 5)]
        public async Task WriteAsync_ShouldStartExactNumberOfWorkers_NumberOfInsertionWorkersGreaterThanZero(int insertionWorkers, int retryWorkers)
        {
            // Arrange
            _workerFactoryMock.Setup(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WorkerResult(100, 0));
            _workerFactoryMock.Setup(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WorkerResult(100, 0));
            var writer = new DocumentsWriter(insertionWorkers, retryWorkers, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            await writer.WriteAsync(CancellationToken.None);
            
            // Assert
            _workerFactoryMock.Verify(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                It.IsAny<ILogger>(), It.IsAny<CancellationToken>()), Times.Exactly(insertionWorkers));
            _workerFactoryMock.Verify(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                It.IsAny<ILogger>(), It.IsAny<CancellationToken>()), Times.Exactly(retryWorkers));
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(10, 5)]
        [InlineData(10, 10)]
        public async Task WriteAsync_ShouldReturnSumOfWorkersResults_InsertionAndRetryWorkersAreStarted(int insertionWorkers, int retryWorkers)
        {
            // Arrange
            var result = new WorkerResult(3L, 1L);
            _workerFactoryMock.Setup(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(result);
            _workerFactoryMock.Setup(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(result);
            
            var writer = new DocumentsWriter(insertionWorkers, retryWorkers, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            var actual = await writer.WriteAsync(CancellationToken.None);
            
            // Assert
            var expected = new TransferResults(result.Successful * insertionWorkers, result.Successful * retryWorkers,
                result.Failed * (insertionWorkers + retryWorkers));
            actual.Should().Be(expected);
        }
        
        [Fact]
        public async Task WriteAsync_ShouldPassNullToRetryChannelForInsertionWorkers_RetryWorkersCountIsZero()
        {
            // Arrange
            _workerFactoryMock.Setup(f => f.RunWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<WorkerResult>());
            _workerFactoryMock.Setup(f => f.RunRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<WorkerResult>());
            
            var writer = new DocumentsWriter(10, 0, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            await writer.WriteAsync(CancellationToken.None);
            
            // Assert
            _workerFactoryMock.Verify(f => f.RunWorker(null, It.IsAny<ILogger>(), It.IsAny<CancellationToken>()),
                Times.Exactly(10));
        }
    }
}