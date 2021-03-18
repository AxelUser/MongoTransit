using System.Collections.Generic;
using System.Threading.Channels;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Progress;
using MongoTransit.Storage.Destination;
using Serilog;

namespace MongoTransit.Workers
{
    public class WorkerPoolFactory : IWorkerPoolFactory
    {
        private readonly int _insertionWorkersCount;
        private readonly int _retryWorkersCount;
        private readonly string _collectionName;
        private readonly IDestinationRepositoryFactory _destinationRepositoryFactory;
        private readonly ILogger _logger;

        public WorkerPoolFactory(int insertionWorkersCount,
            int retryWorkersCount,
            string collectionName,
            IDestinationRepositoryFactory destinationRepositoryFactory,
            ILogger logger)
        {
            _insertionWorkersCount = insertionWorkersCount;
            _retryWorkersCount = retryWorkersCount;
            _collectionName = collectionName;
            _destinationRepositoryFactory = destinationRepositoryFactory;
            _logger = logger;
        }
        
        public IWorkerPool Create(ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader, ProgressNotifier notifier, bool upsert, bool dryRun)
        {
            return new WorkerPool(_insertionWorkersCount, _retryWorkersCount, _collectionName,
                _destinationRepositoryFactory, batchReader, notifier, upsert, dryRun, _logger);
        }
    }
}