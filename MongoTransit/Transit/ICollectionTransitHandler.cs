using System.Threading;
using System.Threading.Tasks;

namespace MongoTransit.Transit
{
    public interface ICollectionTransitHandler
    {
        Task TransitAsync(bool dryRun, CancellationToken token);
    }
}