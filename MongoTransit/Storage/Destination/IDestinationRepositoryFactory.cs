using Serilog;

namespace MongoTransit.Storage.Destination
{
    public interface IDestinationRepositoryFactory
    {
        IDestinationRepository Create(ILogger logger);
    }
}