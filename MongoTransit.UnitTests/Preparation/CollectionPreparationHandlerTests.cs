using System;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using AutoFixture.Xunit2;
using FluentAssertions;
using MongoTransit.Notifications.Notifiers;
using MongoTransit.Options;
using MongoTransit.Preparation;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using MongoTransit.Storage.Source.Models;
using Moq;
using Serilog;
using Xunit;

namespace MongoTransit.UnitTests.Preparation
{
    public class CollectionPreparationHandlerTests
    {
        private readonly CollectionPreparationHandler _sut;
        private readonly Mock<ITextStatusNotifier> _statusNotifierMock;
        private readonly Mock<IDestinationRepository> _destinationMock;
        private readonly Mock<ISourceRepository> _sourceMock;
        private readonly IFixture _fixture;

        public CollectionPreparationHandlerTests()
        {
            _destinationMock = new Mock<IDestinationRepository>();
            _sourceMock = new Mock<ISourceRepository>();
            var loggerMock = new Mock<ILogger>();
            _statusNotifierMock = new Mock<ITextStatusNotifier>();
            _fixture = new Fixture();

            _sut = new CollectionPreparationHandler("test", _destinationMock.Object, _sourceMock.Object,
                loggerMock.Object);
        }
            
        #region Full restore

        [Fact]
        public async Task PrepareCollectionAsync_ShouldRemoveAllDocumentsInDestination_FullRestore()
        {
            // Act
            await _sut.PrepareCollectionAsync(null, _statusNotifierMock.Object,
                CancellationToken.None);
            
            // Assert
            _destinationMock.Verify(r => r.DeleteAllDocumentsAsync(It.IsAny<CancellationToken>()), Times.Once);
        }
        
        [Theory, AutoData]
        public async Task PrepareCollectionAsync_ShouldReturnDocumentsCountInSource_FullRestore(int count)
        {
            // Arrange
            _sourceMock.Setup(s => s.CountAllAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(count);
            
            // Act
            var actual = await _sut.PrepareCollectionAsync(null, _statusNotifierMock.Object,
                CancellationToken.None);
            
            // Assert
            actual.Count.Should().Be(count);
        }
        
        [Fact]
        public async Task PrepareCollectionAsync_ShouldReturnEmptyFilter_FullRestore()
        {
            // Act
            var actual = await _sut.PrepareCollectionAsync(null, _statusNotifierMock.Object,
                CancellationToken.None);
            
            // Assert
            actual.Filter.Should().Be(SourceFilter.Empty);
        }
        
        [Fact]
        public async Task PrepareCollectionAsync_UpdateProgressWhenDeletingDocuments_FullRestore()
        {
            // Act
            await _sut.PrepareCollectionAsync(null, _statusNotifierMock.Object,
                CancellationToken.None);
            
            // Assert
            _statusNotifierMock.VerifySet(p => p.Status = "Removing documents from destination...", Times.Once);
        }
        
        [Fact]
        public async Task PrepareCollectionAsync_UpdateProgressWhenCountingDocuments_FullRestore()
        {
            // Act
            await _sut.PrepareCollectionAsync(null, _statusNotifierMock.Object,
                CancellationToken.None);
            
            // Assert
            _statusNotifierMock.VerifySet(p => p.Status = "Counting documents...", Times.Once);
        }

        #endregion

        #region Iterative restore

        [Theory, AutoData]
        public async Task PrepareCollectionAsync_ShouldReturnFilterByLastCheckpointFromDestination_IterativeRestoreWithNotEmptyDestination(DateTime lastCheckpoint)
        {
            // Arrange
            var options = new IterativeTransitOptions("Modified", TimeSpan.Zero, null);
            _destinationMock.Setup(d => d.FindLastCheckpointAsync("Modified", It.IsAny<CancellationToken>()))
                .ReturnsAsync(lastCheckpoint);
            
            // Act
            var actual = await _sut.PrepareCollectionAsync(options, _statusNotifierMock.Object,
                CancellationToken.None);
            
            // Assert
            actual.Filter.Should().Be(new SourceFilter("Modified", lastCheckpoint));
        }
        
        [Theory, AutoData]
        public async Task PrepareCollectionAsync_ShouldReturnLagBetweenSourceAndDestination_IterativeRestoreWithNotEmptyDestination(int lag)
        {
            // Arrange
            var options = new IterativeTransitOptions("Modified", TimeSpan.Zero, null);
            var lastCheckpoint = _fixture.Create<DateTime>();
            _destinationMock.Setup(d => d.FindLastCheckpointAsync("Modified", It.IsAny<CancellationToken>()))
                .ReturnsAsync(lastCheckpoint);
            _sourceMock.Setup(s =>
                    s.CountLagAsync(new SourceFilter("Modified", lastCheckpoint), It.IsAny<CancellationToken>()))
                .ReturnsAsync(lag);
            
            // Act
            var actual = await _sut.PrepareCollectionAsync(options, _statusNotifierMock.Object,
                CancellationToken.None);
            
            // Assert
            actual.Count.Should().Be(lag);
        }
        
        [Fact]
        public async Task PrepareCollectionAsync_ShouldNotPerformAnyDeletionsInDestination_IterativeRestore()
        {
            // Arrange
            var options = new IterativeTransitOptions("Modified", TimeSpan.Zero, null);
            _destinationMock.Setup(d => d.FindLastCheckpointAsync("Modified", It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<DateTime>());
            _sourceMock.Setup(s =>
                    s.CountLagAsync(It.IsAny<SourceFilter>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<int>());
            
            // Act
            await _sut.PrepareCollectionAsync(options, _statusNotifierMock.Object, CancellationToken.None);
            
            // Assert
            _destinationMock.Verify(d => d.DeleteAllDocumentsAsync(It.IsAny<CancellationToken>()), Times.Never());
        }
        
        [Fact]
        public async Task PrepareCollectionAsync_ShouldUpdateProgressWhileSearchingCheckpoint_IterativeRestore()
        { 
            // Arrange
            var options = new IterativeTransitOptions("Modified", TimeSpan.Zero, null);
            _destinationMock.Setup(d => d.FindLastCheckpointAsync("Modified", It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<DateTime>());
            _sourceMock.Setup(s =>
                    s.CountLagAsync(It.IsAny<SourceFilter>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<int>());
            
            // Act
            await _sut.PrepareCollectionAsync(options, _statusNotifierMock.Object, CancellationToken.None);
            
            // Assert
            _statusNotifierMock.VerifySet(n => n.Status = "Searching checkpoint...");
        }
        
        [Fact]
        public async Task PrepareCollectionAsync_ShouldUpdateProgressWhileCountingLag_IterativeRestore()
        {
            // Arrange
            var options = new IterativeTransitOptions("Modified", TimeSpan.Zero, null);
            _destinationMock.Setup(d => d.FindLastCheckpointAsync("Modified", It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<DateTime>());
            _sourceMock.Setup(s =>
                    s.CountLagAsync(It.IsAny<SourceFilter>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(_fixture.Create<int>());
            
            // Act
            await _sut.PrepareCollectionAsync(options, _statusNotifierMock.Object, CancellationToken.None);
            
            // Assert
            _statusNotifierMock.VerifySet(n => n.Status = "Counting documents...");
        }

        #endregion
    }
}