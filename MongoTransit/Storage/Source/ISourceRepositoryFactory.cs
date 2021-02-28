using Serilog;

namespace MongoTransit.Storage.Source
{
    public interface ISourceRepositoryFactory
    {
        ISourceRepository Create(ILogger logger);
    }
}