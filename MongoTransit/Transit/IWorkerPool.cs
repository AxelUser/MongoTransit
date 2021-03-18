using System.Threading;
using System.Threading.Tasks;

namespace MongoTransit.Transit
{
    public interface IWorkerPool
    {
        void Start(CancellationToken token);
        Task<(long processed, long retried, long failed)> StopAsync();
    }
}