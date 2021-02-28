using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using AutoFixture;
using AutoFixture.Xunit2;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Progress;
using MongoTransit.Storage.Destination;
using MongoTransit.Workers;
using Moq;
using Nito.AsyncEx;
using Serilog;
using Xunit;

namespace MongoTransit.UnitTests
{
    public class DocumentsWriterTests
    {
        private DocumentsWriter _writer;
        private readonly Fixture _fixture;
        private readonly Mock<IDestinationRepository> _destMock;
        private readonly Channel<List<ReplaceOneModel<BsonDocument>>> _channel;
        private readonly Mock<IDestinationRepositoryFactory> _destFactoryMock;
        private readonly Mock<IProgressNotifier> _notifierMock;
        private readonly Mock<ILogger> _loggerMock;

        public DocumentsWriterTests()
        {
            _fixture = new Fixture();
            
            _destFactoryMock = new Mock<IDestinationRepositoryFactory>();
            _destMock = new Mock<IDestinationRepository>();
            _destFactoryMock.Setup(factory => factory.Create(It.IsAny<ILogger>())).Returns(_destMock.Object);

            _channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();

            _notifierMock = new Mock<IProgressNotifier>();
            _loggerMock = new Mock<ILogger>();

            _writer = new DocumentsWriter(4, 4, _fixture.Create<string>(), _destFactoryMock.Object, _channel,
                _notifierMock.Object, true, false, _loggerMock.Object);
        }

        [Theory, AutoData]
        public async Task WriteAsync_ShouldStartExactNumberOfInsertionWorkers(int numberOfWorkers)
        {
            // Arrange
            var countdown = new AsyncCountdownEvent(numberOfWorkers);
            var fence = new AsyncManualResetEvent();
            
            _writer = new DocumentsWriter(numberOfWorkers, numberOfWorkers, _fixture.Create<string>(), _destFactoryMock.Object, _channel,
                _notifierMock.Object, true, false, _loggerMock.Object);
            _destMock.Setup(repository => repository.ReplaceManyAsync(It.IsAny<List<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    countdown.Signal();
                    await fence.WaitAsync();
                });
            
            // Act
            foreach (var _ in Enumerable.Range(0, numberOfWorkers))
            {
                await _channel.Writer.WriteAsync(Enumerable
                    .Repeat(new ReplaceOneModel<BsonDocument>(new BsonDocument(), new BsonDocument()), 3).ToList());
            }

            var cts = new CancellationTokenSource();
            var write = _writer.WriteAsync(cts.Token);
            await countdown.WaitAsync(default);
            await _channel.Writer.WriteAsync(Enumerable
                .Repeat(new ReplaceOneModel<BsonDocument>(new BsonDocument(), new BsonDocument()), 3).ToList(), default);
            cts.Cancel();
            fence.Set();

            Func<Task> act = async () => await write;

            // Assert
            await act.Should().ThrowAsync<OperationCanceledException>();
            _channel.Reader.Count.Should().Be(1);
        }

        [Theory, AutoData]
        public async Task WriteAsync_ShouldWriteAllRequests_NoErrors(int batchCount)
        {
            // Arrange
            foreach (var _ in Enumerable.Range(0, batchCount))
            {
                await _channel.Writer.WriteAsync(Enumerable
                    .Repeat(new ReplaceOneModel<BsonDocument>(new BsonDocument(), new BsonDocument()), 3).ToList());
            }

            // Act
            var write = _writer.WriteAsync(default);
            _channel.Writer.Complete();
            await write;

            // Assert
            _destMock.Verify(
                repository => repository.ReplaceManyAsync(It.IsAny<List<ReplaceOneModel<BsonDocument>>>(),
                    It.IsAny<CancellationToken>()), Times.Exactly(batchCount));
        }
    }
}