using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using AutoFixture;
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
        private CollectionTransitHandler _handler;
        private Fixture _fixture;
        private Mock<ISourceRepository> _sourceMock;
        private Mock<IDestinationRepository> _destMock;
        private Mock<ICollectionPreparationHandler> _collectionPrepareMock;

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
            var workerPoolFactoryMock = new Mock<IWorkerPoolFactory>();
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
        public async Task TransitAsync_ShouldCompletedWithoutRestore_CollectionUpToDate()
        {
            _collectionPrepareMock.Setup(handler => handler.PrepareCollectionAsync(It.IsAny<IterativeTransitOptions?>(),
                It.IsAny<TextStatusProvider>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CollectionPrepareResult(new BsonDocument(), 0));
            await _handler.TransitAsync(false, default);
            _sourceMock.Verify(repository => repository.ReadDocumentsAsync(It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<ChannelWriter<List<ReplaceOneModel<BsonDocument>>>>(), It.IsAny<int>(), It.IsAny<bool>(),
                It.IsAny<string[]>(), It.IsAny<bool>(), It.IsAny<IDocumentFinder>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    }
}