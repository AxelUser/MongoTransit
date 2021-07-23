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
using MongoTransit.Extensions;
using MongoTransit.Progress;
using MongoTransit.Storage.Destination;
using MongoTransit.Workers;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.UnitTests
{
    public class WriteWorkerFactoryTests
    {
        private readonly Fixture _fixture;
        private readonly Channel<List<ReplaceOneModel<BsonDocument>>> _channel;
        private readonly Mock<IDestinationRepository> _destinationRepositoryMock;
        private readonly Mock<IProgressNotifier> _notifierMock;
        
        private readonly WriteWorkerFactory _sut;

        public WriteWorkerFactoryTests()
        {
            _fixture = new Fixture();
            _channel = Channel.CreateUnbounded<List<ReplaceOneModel<BsonDocument>>>();
            _destinationRepositoryMock = new Mock<IDestinationRepository>();
            var destinationRepositoryFactoryMock = new Mock<IDestinationRepositoryFactory>();
            destinationRepositoryFactoryMock.Setup(f => f.Create(It.IsAny<ILogger>()))
                .Returns(_destinationRepositoryMock.Object);
            _notifierMock = new Mock<IProgressNotifier>();
            _sut = new WriteWorkerFactory(_channel, destinationRepositoryFactoryMock.Object, _notifierMock.Object, false,
                "TestCollection");
        }
        
        #region RunWorker

        [Fact]
        public async Task RunWorker_ShouldProcessBatch_BatchIsSentToChannel()
        {
            // Arrange
            var replacements = Enumerable.Range(0, 10).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();

            // Act
            var worker = _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                CancellationToken.None);
            await WriteBatchAndCloseChannelAsync(batch);
            await worker;
            
            // Assert
            _destinationRepositoryMock.Verify(r => r.ReplaceManyAsync(batch, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task RunWorker_ShouldWaitUntilChannelIsClosed_ChannelIsMarkedAsCompleted()
        {
            // Arrange
            var replacements = Enumerable.Range(0, 10).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();

            // Act
            var worker = _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                CancellationToken.None);
            await WriteBatchAndCloseChannelAsync(batch);
            await worker;
            
            // Assert
            worker.Status.Should().Be(TaskStatus.RanToCompletion);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunWorker_ShouldReturnCountOfReplacedDocuments_ReplaceIsProcessedWithoutErrors(int batchSize)
        {
            // Arrange
            var replacements = Enumerable.Range(0, batchSize).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();

            // Act
            await WriteBatchAndCloseChannelAsync(batch);
            var actual = await _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            actual.successful.Should().Be(batchSize);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunWorker_ShouldReturnCountOfFailedDocumentsReplacements_ReplaceIsProcessedWithErrors(int batchSize)
        {
            // Arrange
            var replacements = Enumerable.Range(0, batchSize).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();
            var exception =
                new ReplaceManyException(batch.Select((b, idx) => new ReplaceErrorInfo(idx, "Replace error")).ToList(),
                    batch.Count);
            _destinationRepositoryMock.Setup(r => r.ReplaceManyAsync(batch, It.IsAny<CancellationToken>()))
                .Throws(exception);

            // Act
            await WriteBatchAndCloseChannelAsync(batch);
            var (successful, _, failed) = await _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            successful.Should().Be(0);
            failed.Should().Be(batchSize);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunWorker_ShouldReturnCountOfRetryableDocumentsReplacements_ReplaceIsProcessedWithRetryableErrors(int batchSize)
        {
            // Arrange
            var replacements = Enumerable.Range(0, batchSize).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();
            var exception =
                new ReplaceManyException(
                    batch.Select((b, idx) =>
                        new ReplaceErrorInfo(idx, WriteWorkerFactory.ErrorUpdateWithMoveToAnotherShard)).ToList(),
                    batch.Count);
            _destinationRepositoryMock.Setup(r => r.ReplaceManyAsync(batch, It.IsAny<CancellationToken>()))
                .Throws(exception);

            // Act
            await WriteBatchAndCloseChannelAsync(batch);
            var (successful, retryable, _) = await _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(),
                new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            successful.Should().Be(0);
            retryable.Should().Be(batchSize);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunWorker_ShouldSendRetryableDocumentsIntoRetryChannel_ReplaceIsProcessedWithRetryableErrors(int batchSize)
        {
            // Arrange
            var replacements = Enumerable.Range(0, batchSize).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();
            var exception =
                new ReplaceManyException(
                    batch.Select((b, idx) =>
                        new ReplaceErrorInfo(idx, WriteWorkerFactory.ErrorUpdateWithMoveToAnotherShard)).ToList(),
                    batch.Count);
            _destinationRepositoryMock.Setup(r => r.ReplaceManyAsync(batch, It.IsAny<CancellationToken>()))
                .Throws(exception);
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();

            // Act
            await WriteBatchAndCloseChannelAsync(batch);
            
            await _sut.RunWorker(retryChannel,
                new Mock<ILogger>().Object,
                CancellationToken.None);
            retryChannel.Writer.Complete();

            // Assert
            var actual = await ReadModelsFromChannelAsync(retryChannel);
            actual.Should().BeEquivalentTo(batch);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunWorker_ShouldNotifyAboutReplacedDocuments_BatchIsHandledWithoutErrors(int batchSize)
        {
            // Arrange
            var replacements = Enumerable.Range(0, batchSize).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();

            // Act
            await WriteBatchAndCloseChannelAsync(batch);
            await _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            _notifierMock.Verify(notifier => notifier.Notify(batchSize), Times.Once);
        }
        
        [Fact]
        public async Task RunWorker_ShouldThrowCancellationException_ChannelIsOpenAndOperationIsCancelled()
        {
            // Arrange
            var replacements = Enumerable.Range(0, 10).Select(_ => new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            });
            var batch = replacements.Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d)).ToList();
            var cts = new CancellationTokenSource();
            await _channel.Writer.WriteAsync(batch, CancellationToken.None);
            var worker = _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                cts.Token);

            // Act
            cts.Cancel();
            
            // Assert
            Func<Task> act = async () => await worker;
            await act.Should().ThrowAsync<OperationCanceledException>();
        }

        #endregion

        #region helpers

        private async Task WriteBatchAndCloseChannelAsync(List<ReplaceOneModel<BsonDocument>> batch)
        {
            await _channel.Writer.WriteAsync(batch, CancellationToken.None);
            _channel.Writer.Complete();
        }
        
        private static async Task<List<ReplaceOneModel<BsonDocument>>> ReadModelsFromChannelAsync(ChannelReader<ReplaceOneModel<BsonDocument>> reader)
        {
            var models = new List<ReplaceOneModel<BsonDocument>>();
            await foreach (var model in reader.ReadAllAsync())
                models.Add(model);
            return models;
        }

        #endregion
    }
}