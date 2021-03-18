using System.Threading;
using System.Threading.Tasks;

namespace MongoTransit.Workers
{
    public interface IDocumentsWriter
    {
        Task<TransferResults> WriteAsync(CancellationToken token);
    }
}