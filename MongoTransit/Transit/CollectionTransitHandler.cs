using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Options;
using MongoTransit.Progress;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using MongoTransit.Workers;
using Serilog;

namespace MongoTransit.Transit
{
    public class CollectionTransitHandler
    {
        private readonly IProgressManager _manager;
        private readonly ILogger _logger;
        private readonly CollectionTransitOptions _options;
        private readonly ICollectionPreparationHandler _preparationHandler;
        private readonly IWorkerPoolFactory _workerPoolFactory;
        private readonly IDestinationRepository _destination;
        private readonly ISourceRepository _source;

        public CollectionTransitHandler(ISourceRepositoryFactory sourceRepositoryFactory,
            IDestinationRepositoryFactory destinationRepositoryFactory,
            ICollectionPreparationHandler preparationHandler,
            IWorkerPoolFactory workerPoolFactory,
            IProgressManager manager,
            ILogger logger,
            CollectionTransitOptions options)
        {
            _manager = manager;
            _logger = logger;
            _options = options;

            _destination = destinationRepositoryFactory.Create(_logger);
            _source = sourceRepositoryFactory.Create(_logger);
            
            _preparationHandler = preparationHandler;
            _workerPoolFactory = workerPoolFactory;
        }
        
        public async Task TransitAsync(bool dryRun, CancellationToken token)
        {
            _logger.Debug("Starting transit operation");
            var swTransit = new Stopwatch();
            swTransit.Start();

            var status = new TextStatusProvider("Checking...");
            _manager.Attach(_options.Collection, status);
            try
            {
                await InternalTransit(dryRun, status, token);
            }
            finally
            {
                swTransit.Stop();
                _logger.Debug("Transit finished in {elapsed}", swTransit.Elapsed);
                _manager.Detach(_options.Collection);
            }
        }

        private async Task InternalTransit(bool dryRun, TextStatusProvider progress, CancellationToken token)
        {
            var sw = new Stopwatch();
            sw.Start();
            
            var transitChannel = Channel.CreateBounded<List<ReplaceOneModel<BsonDocument>>>(_options.Workers);

            var (filter, count) = await _preparationHandler.PrepareCollectionAsync(_options.IterativeTransferOptions, progress, token);
            
            if (count == 0)
            {
                _logger.Information("Collection {Collection} is up-to date, skipping transit", _options.Collection);
                return;
            }

            var notifier = new ProgressNotifier(count);
            _manager.Attach(_options.Collection, notifier);

            var workerPool = _workerPoolFactory.Create(transitChannel, notifier, _options.Upsert, dryRun);

            workerPool.Start(token);

            await _source.ReadDocumentsAsync(filter, transitChannel, _options.BatchSize,
                _options.FetchKeyFromDestination, _options.KeyFields ?? Array.Empty<string>(), _options.Upsert,
                _destination, token);

            var (processed, retried, failed) = await workerPool.StopAsync();
            sw.Stop();
            
            _logger.Debug("Transfer was completed in {Elapsed}", sw.Elapsed);
            
            _logger.Information("Transferred {S}; Retried {R}; Failed {F};", processed, retried, failed);
        }
    }
}