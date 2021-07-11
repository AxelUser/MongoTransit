using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using AutoFixture;
using AutoFixture.Xunit2;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Options;
using MongoTransit.Progress;
using MongoTransit.Storage;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using MongoTransit.Transit;
using MongoTransit.Workers;
using Xunit;
using Moq;
using Serilog;

namespace MongoTransit.UnitTests
{
    public class CollectionTransitHandlerTests
    {
        private readonly CollectionTransitHandler _handler;
        private readonly Fixture _fixture;
        private readonly Mock<ISourceRepository> _sourceMock;
        private readonly Mock<IDestinationRepository> _destMock;
        private readonly Mock<ICollectionPreparationHandler> _collectionPrepareMock;
        private readonly Mock<IDocumentsWriter> _workerPoolMock;

        public CollectionTransitHandlerTests()
        {
            _fixture = new Fixture();
            var sourceFactoryMock = new Mock<ISourceRepositoryFactory>();
            _sourceMock = new Mock<ISourceRepository>();
            sourceFactoryMock.Setup(factory => factory.Create(It.IsAny<ILogger>())).Returns(_sourceMock.Object);
            
            var destFactoryMock = new Mock<IDestinationRepositoryFactory>();
            _destMock = new Mock<IDestinationRepository>();
            destFactoryMock.Setup(factory => factory.Create(It.IsAny<ILogger>())).Returns(_destMock.Object);
            
            _collectionPrepareMock = new Mock<ICollectionPreparationHandler>();
            
            var workerPoolFactoryMock = new Mock<IDocumentsWriterFactory>();
            _workerPoolMock = new Mock<IDocumentsWriter>();
            workerPoolFactoryMock.Setup(factory =>
                    factory.Create(It.IsAny<ChannelReader<List<ReplaceOneModel<BsonDocument>>>>(),
                        It.IsAny<ProgressNotifier>(), It.IsAny<bool>(), It.IsAny<bool>()))
                .Returns(_workerPoolMock.Object);
            
            var progressManagerMock = new Mock<IProgressManager>();
            var loggerMock = new Mock<ILogger>();
            var options = _fixture.Create<CollectionTransitOptions>();
            _handler = new CollectionTransitHandler(sourceFactoryMock.Object,
                destFactoryMock.Object,
                _collectionPrepareMock.Object,
                workerPoolFactoryMock.Object,
                progressManagerMock.Object,
                loggerMock.Object,
                options);
        }
        
        [Fact]
        public async Task TransitAsync_ShouldCompleteWithoutRestore_CollectionUpToDate()
        {
            // Arrange
            _collectionPrepareMock.Setup(handler => handler.PrepareCollectionAsync(It.IsAny<IterativeTransitOptions?>(),
                It.IsAny<TextStatusProvider>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CollectionPrepareResult(SourceFilter.Empty, 0));
            
            // Act
            await _handler.TransitAsync(false, default);
            
            // Assert
            _sourceMock.Verify(repository => repository.ReadDocumentsAsync(It.IsAny<SourceFilter>(),
                It.IsAny<ChannelWriter<List<ReplaceOneModel<BsonDocument>>>>(), It.IsAny<int>(), It.IsAny<bool>(),
                It.IsAny<string[]>(), It.IsAny<bool>(), It.IsAny<IDestinationDocumentFinder>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Theory, AutoData]
        public async Task TransitAsync_ShouldCompletedWithAllStepsOfRestore_CollectionHasLag(TransferResults expectedResults)
        {
            // Arrange
            _collectionPrepareMock.Setup(handler => handler.PrepareCollectionAsync(It.IsAny<IterativeTransitOptions?>(),
                    It.IsAny<TextStatusProvider>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CollectionPrepareResult(SourceFilter.Empty, 100));
            _workerPoolMock.Setup(pool => pool.WriteAsync(It.IsAny<CancellationToken>())).ReturnsAsync(expectedResults);
            
            // Act
            await _handler.TransitAsync(false, default);
            
            // Assert
            _sourceMock.Verify(repository => repository.ReadDocumentsAsync(SourceFilter.Empty,
                It.IsAny<ChannelWriter<List<ReplaceOneModel<BsonDocument>>>>(), It.IsAny<int>(), It.IsAny<bool>(),
                It.IsAny<string[]>(), It.IsAny<bool>(), It.IsAny<IDestinationDocumentFinder>(), It.IsAny<CancellationToken>()));
            _workerPoolMock.Verify(pool => pool.WriteAsync(It.IsAny<CancellationToken>()));
        }
    }
}