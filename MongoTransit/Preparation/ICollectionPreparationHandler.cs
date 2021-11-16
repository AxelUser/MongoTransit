using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Configuration.Options;
using MongoTransit.Notifications.Notifiers;

namespace MongoTransit.Preparation
{
    public interface ICollectionPreparationHandler
    {
        Task<CollectionPrepareResult> PrepareCollectionAsync(IterativeTransitOptions? iterativeTransitOptions,
            ITextStatusNotifier progress,
            CancellationToken token);
    }
}