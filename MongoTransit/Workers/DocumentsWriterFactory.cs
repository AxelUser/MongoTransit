using System.Collections.Generic;
using System.Threading.Channels;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Progress;
using MongoTransit.Storage.Destination;
using Serilog;

namespace MongoTransit.Workers
{
    public class DocumentsWriterFactory : IDocumentsWriterFactory
    {
        private readonly int _insertionWorkersCount;
        private readonly int _retryWorkersCount;
        private readonly string _collectionName;
        private readonly IDestinationRepositoryFactory _destinationRepositoryFactory;
        private readonly ILogger _logger;

        public DocumentsWriterFactory(int insertionWorkersCount,
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
        
        public IDocumentsWriter Create(ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader,
            IProgressNotifier notifier, bool upsert, bool dryRun)
        {
            var workerFactory = new WriteWorkerFactory(batchReader, _destinationRepositoryFactory, notifier, upsert,
                dryRun, _collectionName);
            return new DocumentsWriter(_insertionWorkersCount, _retryWorkersCount, _collectionName,
                workerFactory, _logger);
        }
    }
}