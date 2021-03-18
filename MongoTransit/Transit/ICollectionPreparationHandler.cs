using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoTransit.Options;
using MongoTransit.Progress;

namespace MongoTransit.Transit
{
    public interface ICollectionPreparationHandler
    {
        Task<CollectionPrepareResult> PrepareCollectionAsync(IterativeTransitOptions? iterativeTransitOptions,
            TextStatusProvider progress,
            CancellationToken token);
    }
}