using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Storage.Destination;
using Moq;
using Serilog;

namespace MongoTransit.IntegrationTests
{
    public class DestinationRepositoryTests: RepositoriesTestBase
    {
        
        private readonly IMongoCollection<BsonDocument> _destCollection;
        private readonly DestinationRepository _sut;
        
        public DestinationRepositoryTests()
        {
            var (_, collection) = CreateConnection(nameof(DestinationRepositoryTests));
            _destCollection = collection;

            _sut = new DestinationRepository(_destCollection, new Mock<ILogger>().Object);
        }
    }
}