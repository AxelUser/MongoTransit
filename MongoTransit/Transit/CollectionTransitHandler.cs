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
        private readonly ProgressManager _manager;
        private readonly ILogger _logger;
        private readonly CollectionTransitOptions _options;
        private readonly IDestinationRepositoryFactory _destinationFactory;
        private readonly IDestinationRepository _destination;
        private readonly ISourceRepository _source;

        public CollectionTransitHandler(ISourceRepositoryFactory sourceRepositoryFactory,
            IDestinationRepositoryFactory destinationRepositoryFactory,
            ProgressManager manager,
            ILogger logger,
            CollectionTransitOptions options)
        {
            _manager = manager;
            _logger = logger;
            _options = options;

            _destinationFactory = destinationRepositoryFactory;
            _destination = _destinationFactory.Create(_logger);
            _source = sourceRepositoryFactory.Create(_logger);
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
            var transitChannel = Channel.CreateBounded<List<ReplaceOneModel<BsonDocument>>>(_options.Workers);

            var (filter, count) = await PrepareCollectionAsync(progress, token);
            sw.Stop();
            _logger.Debug("Collection check was completed in {elapsed} ms", sw.ElapsedMilliseconds);

            if (count == 0)
            {
                _logger.Information("Collection {Collection} is up-to date, skipping transit", _options.Collection);
                return;
            }

            var notifier = new ProgressNotifier(count);
            _manager.Attach(_options.Collection, notifier);

            var workerPool = new WorkerPool(_options.Workers * Environment.ProcessorCount,
                _options.Workers * Environment.ProcessorCount, _options.Collection,
                _destinationFactory, transitChannel, notifier, _options.Upsert, dryRun, _logger);
            
            sw.Restart();

            workerPool.Start(token);

            await _source.ReadDocumentsAsync(filter, transitChannel, _options.BatchSize,
                _options.FetchKeyFromDestination, _options.KeyFields ?? Array.Empty<string>(), _options.Upsert,
                _destination, token);

            var (processed, retried, failed) = await workerPool.StopAsync();
            
            _logger.Debug("Transfer was completed in {elapsed}", sw.Elapsed);
            
            _logger.Information("Transferred {S}; Retried {R}; Failed {F};", processed, retried, failed);
        }

        private async Task<(BsonDocument filter, long count)> PrepareCollectionAsync(TextStatusProvider progress,
            CancellationToken token)
        {
            if (_options.IterativeTransferOptions != null)
            {
                return await CheckIterativeCollectionAsync(progress, _options.IterativeTransferOptions, token);
            }
            
            var filter = new BsonDocument();
            progress.Status = "Removing documents from destination...";
            await _destination.DeleteAllDocumentsAsync(token);
            progress.Status = "Counting documents...";
            var count = await _source.CountAllDocumentsAsync(token);
            return (filter, count);
        }

        private async Task<(BsonDocument filter, long count)> CheckIterativeCollectionAsync(TextStatusProvider progress, IterativeTransitOptions iterOpts, CancellationToken token)
        {
            _logger.Debug("Detected iterative transit option. Fetching checkpoint and lag");

            var (checkpointField, offset, forcedCheckpoint) = iterOpts;

            progress.Status = "Searching checkpoint...";

            DateTime? lastCheckpoint;
            
            if (forcedCheckpoint != null)
            {
                _logger.Information("Forced to use checkpoint {ForcedCheckpoint} for collection {Collection}",
                    iterOpts.ForcedCheckpoint, _options.Collection);
                lastCheckpoint = iterOpts.ForcedCheckpoint;
            }
            else
            {
                _logger.Debug("Fetching last checkpoint for collection {Collection}", _options.Collection);
                lastCheckpoint = await _destination.FindLastCheckpointAsync(checkpointField, token);
                lastCheckpoint -= offset;
            }
            
            if (lastCheckpoint == null)
            {
                throw new Exception($"Couldn't get checkpoint for collection {_options.Collection}");
            }

            _logger.Debug("Counting how many documents should be transferred");
            progress.Status = "Counting documents...";
            var count = await _source.CountLagAsync(checkpointField, lastCheckpoint.Value, token);

            _logger.Debug("Collection {Collection} has checkpoint {LastCheckpoint} and lag {Lag:N0}",
                _options.Collection, lastCheckpoint, count);
            
            var filter = new BsonDocument(checkpointField, new BsonDocument("$gte", lastCheckpoint));

            return (filter, count);
        }
    }
}