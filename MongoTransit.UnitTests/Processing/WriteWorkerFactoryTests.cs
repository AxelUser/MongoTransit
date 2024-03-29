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
using MongoTransit.Notifications.Notifiers;
using MongoTransit.Processing.Workers;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Destination.Exceptions;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.UnitTests.Processing
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
            var actual = await _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            actual.Successful.Should().Be(batchSize);
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
            var (successful, failed) = await _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(), new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            successful.Should().Be(0);
            failed.Should().Be(batchSize);
        }
        
        [Fact]
        public async Task RunWorker_ShouldReturnZeroCountOfSuccessfulAndFailedDocumentsReplacements_ReplaceIsProcessedWithRetryableErrors_HasRetryChannel()
        {
            // Arrange
            var replacements = Enumerable.Range(0, 100).Select(_ => new BsonDocument
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
            var (successful, failed) = await _sut.RunWorker(Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>(),
                new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            successful.Should().Be(0);
            failed.Should().Be(0);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunWorker_ShouldReturnCountOfFailedDocumentsReplacements_ReplaceIsProcessedWithRetryableErrors_NullRetryChannel(int batchSize)
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
            var (successful, failed) = await _sut.RunWorker(null, new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            failed.Should().Be(batchSize);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunWorker_ShouldSendRetryableDocumentsIntoRetryChannel_ReplaceIsProcessedWithRetryableErrors_HasRetryChannel(int batchSize)
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
            
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
            await WriteBatchAndCloseChannelAsync(_channel, batch);
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

        #region RunRetryWorker

        [Fact]
        public async Task RunRetryWorker_ShouldProcessReplacements_ModelsAreSentToChannel()
        {
            // Arrange
            var retries = Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();

            // Act
            var worker = _sut.RunRetryWorker(retryChannel, new Mock<ILogger>().Object,
                CancellationToken.None);
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            await worker;
            
            // Assert
            _destinationRepositoryMock.Verify(r => r.ReplaceDocumentAsync(It.IsIn(retries), It.IsAny<CancellationToken>()),
                Times.Exactly(retries.Length));
        }

        [Fact]
        public async Task RunRetryWorker_ShouldWaitUntilChannelIsClosed_ChannelIsMarkedAsCompleted()
        {
            // Arrange
            var retries = Enumerable.Range(0, 10)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();

            // Act
            var worker = _sut.RunRetryWorker(retryChannel, new Mock<ILogger>().Object,
                CancellationToken.None);
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            await worker;
            
            // Assert
            worker.Status.Should().Be(TaskStatus.RanToCompletion);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunRetryWorker_ShouldReturnCountOfReplacedDocuments_ReplacesAreProcessedWithoutErrors(int retriesCount)
        {
            // Arrange
            var retries = Enumerable.Range(0, retriesCount)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();

            // Act
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            var result = await _sut.RunRetryWorker(retryChannel, new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            result.Successful.Should().Be(retriesCount);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunRetryWorker_ShouldReturnZeroOfSuccessfulDocumentsReplacements_ReplacesAreProcessedWithErrors(int retriesCount)
        {
            // Arrange
            var retries = Enumerable.Range(0, retriesCount)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
            _destinationRepositoryMock.Setup(r => r.ReplaceDocumentAsync(It.IsIn(retries), It.IsAny<CancellationToken>()))
                .Throws(new ReplaceOneException("Error", new Exception("Inner")));

            // Act
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            var result = await _sut.RunRetryWorker(retryChannel, new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            result.Successful.Should().Be(0);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunRetryWorker_ShouldReturnCountOfFailedDocumentsReplacements_ReplacesAreProcessedWithErrors(int retriesCount)
        {
            // Arrange
            var retries = Enumerable.Range(0, retriesCount)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
            _destinationRepositoryMock.Setup(r => r.ReplaceDocumentAsync(It.IsIn(retries), It.IsAny<CancellationToken>()))
                .Throws(new ReplaceOneException("Error", new Exception("Inner")));

            // Act
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            var result = await _sut.RunRetryWorker(retryChannel, new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            result.Failed.Should().Be(retriesCount);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunRetryWorker_ShouldReturnZeroOfSuccessfulDocumentsReplacements_ReplacesAreProcessedWithRetryableErrors(int retriesCount)
        {
            // Arrange
            var retries = Enumerable.Range(0, retriesCount)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
            _destinationRepositoryMock
                .Setup(r => r.ReplaceDocumentAsync(It.IsIn(retries), It.IsAny<CancellationToken>()))
                .Throws(new ReplaceOneException(WriteWorkerFactory.ErrorUpdateWithMoveToAnotherShard,
                    new Exception(WriteWorkerFactory.ErrorUpdateWithMoveToAnotherShard)));

            // Act
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            var result = await _sut.RunRetryWorker(retryChannel,
                new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            result.Successful.Should().Be(0);
        }
        
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunRetryWorker_ShouldReturnCountOfFailedDocumentsReplacementsAndZeroRetries_ReplacesAreProcessedWithRetryableErrors(int retriesCount)
        {
            // Arrange
            var retries = Enumerable.Range(0, retriesCount)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
            _destinationRepositoryMock
                .Setup(r => r.ReplaceDocumentAsync(It.IsIn(retries), It.IsAny<CancellationToken>()))
                .Throws(new ReplaceOneException(WriteWorkerFactory.ErrorUpdateWithMoveToAnotherShard,
                    new Exception(WriteWorkerFactory.ErrorUpdateWithMoveToAnotherShard)));

            // Act
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            var result = await _sut.RunRetryWorker(retryChannel,
                new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            result.Failed.Should().Be(retriesCount);
        }

        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task RunRetryWorker_ShouldNotifyAboutReplacedDocuments_BatchIsHandledWithoutErrors(int retriesCount)
        {
            // Arrange
            var retries = Enumerable.Range(0, retriesCount)
                .Select(_ => new BsonDocument
                {
                    ["_id"] = _fixture.Create<string>(),
                    ["Value"] = _fixture.Create<string>()
                })
                .Select(d => new ReplaceOneModel<BsonDocument>(d.GetFilterBy("_id"), d))
                .ToArray();
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();

            // Act
            await WriteBatchAndCloseChannelAsync(retryChannel, retries);
            await _sut.RunRetryWorker(retryChannel, new Mock<ILogger>().Object,
                CancellationToken.None);

            // Assert
            _notifierMock.Verify(notifier => notifier.Notify(1), Times.Exactly(retriesCount));
        }
        
        [Fact]
        public async Task RunRetryWorker_ShouldThrowCancellationException_ChannelIsOpenAndOperationIsCancelled()
        {
            // Arrange
            var retry = new BsonDocument
            {
                ["_id"] = _fixture.Create<string>(),
                ["Value"] = _fixture.Create<string>()
            }; 
            
            var retryChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
            var cts = new CancellationTokenSource();
            await retryChannel.Writer.WriteAsync(new ReplaceOneModel<BsonDocument>(retry.GetFilterBy("_id"), retry),
                CancellationToken.None);
            var worker = _sut.RunRetryWorker(retryChannel, new Mock<ILogger>().Object,
                cts.Token);

            // Act
            cts.Cancel();
            
            // Assert
            Func<Task> act = async () => await worker;
            await act.Should().ThrowAsync<OperationCanceledException>();
        }

        #endregion

        #region helpers

        private static async Task WriteBatchAndCloseChannelAsync<T>(ChannelWriter<T> channel, params T[] messages)
        {
            foreach (var message in messages)
            {
                await channel.WriteAsync(message, CancellationToken.None);                
            }

            channel.Complete();
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