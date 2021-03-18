using Serilog;

namespace MongoTransit.Storage
{
    public interface IDestinationRepositoryFactory
    {
        DestinationRepository Create(ILogger logger);
    }
}