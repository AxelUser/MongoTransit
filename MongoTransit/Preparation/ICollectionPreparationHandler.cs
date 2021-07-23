using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Options;
using MongoTransit.Progress;

namespace MongoTransit.Preparation
{
    public interface ICollectionPreparationHandler
    {
        Task<CollectionPrepareResult> PrepareCollectionAsync(IterativeTransitOptions? iterativeTransitOptions,
            ITextStatusNotifier progress,
            CancellationToken token);
    }
}