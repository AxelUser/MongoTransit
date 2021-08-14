using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Processing;

namespace MongoTransit.Transit
{
    public interface ICollectionTransitHandler
    {
        Task<TransferResults> TransitAsync(bool dryRun, CancellationToken token);
    }
}