using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Notifications.Notifiers;
using MongoTransit.Options;

namespace MongoTransit.Preparation
{
    public interface ICollectionPreparationHandler
    {
        Task<CollectionPrepareResult> PrepareCollectionAsync(IterativeTransitOptions? iterativeTransitOptions,
            ITextStatusNotifier progress,
            CancellationToken token);
    }
}