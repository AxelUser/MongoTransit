using System.Threading;
using System.Threading.Tasks;

namespace MongoTransit.Processing
{
    public interface IDocumentsWriter
    {
        Task<TransferResults> WriteAsync(CancellationToken token);
    }
}