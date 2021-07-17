using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using AutoFixture;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Workers;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.UnitTests
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
            _workerFactoryMock.Setup(f => f.CreateWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((100, 0, 0));
            _workerFactoryMock.Setup(f => f.CreateRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((100, 0, 0));
            _writer = new DocumentsWriter(numberOfWorkers, 0, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            await _writer.WriteAsync(CancellationToken.None);
            
            // Assert
            _workerFactoryMock.Verify(f => f.CreateWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                It.IsAny<ILogger>(), It.IsAny<CancellationToken>()), Times.Exactly(numberOfWorkers));
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task WriteAsync_ShouldStartExactNumberOfRetryWorkers_NumberOfInsertionAndRetryWorkersGreaterThanZero(int numberOfWorkers)
        {
            // Arrange
            _workerFactoryMock.Setup(f => f.CreateWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((100L, 0L, 0L));
            _workerFactoryMock.Setup(f => f.CreateRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((100L, 0L, 0L));
            _writer = new DocumentsWriter(numberOfWorkers, numberOfWorkers, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            await _writer.WriteAsync(CancellationToken.None);
            
            // Assert
            _workerFactoryMock.Verify(f => f.CreateRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                It.IsAny<ILogger>(), It.IsAny<CancellationToken>()), Times.Exactly(numberOfWorkers));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        public async Task WriteAsync_ShouldReturnSumOfWorkersResults_InsertionAndRetryWorkersAreStarted(int insertionWorkers)
        {
            // Arrange
            var result = (3L, 2L, 1L);
            _workerFactoryMock.Setup(f => f.CreateWorker(It.IsAny<ChannelWriter<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(result);
            _workerFactoryMock.Setup(f => f.CreateRetryWorker(It.IsAny<ChannelReader<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<ILogger>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(result);
            
            _writer = new DocumentsWriter(insertionWorkers, insertionWorkers, _fixture.Create<string>(),
                _workerFactoryMock.Object, _loggerMock.Object);
            
            // Act
            var actual = await _writer.WriteAsync(CancellationToken.None);
            
            // Assert
            var expected = new TransferResults(result.Item1 * insertionWorkers * 2, result.Item2 * insertionWorkers * 2,
                result.Item3 * insertionWorkers * 2);
            actual.Should().Be(expected);
        }
    }
}