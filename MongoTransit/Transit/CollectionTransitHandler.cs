using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Notifications;
using MongoTransit.Notifications.Notifiers;
using MongoTransit.Options;
using MongoTransit.Preparation;
using MongoTransit.Processing;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using Serilog;

namespace MongoTransit.Transit
{
    public class CollectionTransitHandler : ICollectionTransitHandler
    {
        private readonly IProgressManager _manager;
        private readonly ILogger _logger;
        private readonly CollectionTransitOptions _options;
        private readonly ICollectionPreparationHandler _preparationHandler;
        private readonly IDocumentsWriterFactory _documentsWriterFactory;
        private readonly IDestinationRepository _destination;
        private readonly ISourceRepository _source;

        public CollectionTransitHandler(ISourceRepositoryFactory sourceRepositoryFactory,
            IDestinationRepositoryFactory destinationRepositoryFactory,
            ICollectionPreparationHandler preparationHandler,
            IDocumentsWriterFactory documentsWriterFactory,
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
            _documentsWriterFactory = documentsWriterFactory;
        }
        
        public async Task<TransferResults> TransitAsync(bool dryRun, CancellationToken token)
        {
            _logger.Debug("Starting transit operation");
            var swTransit = new Stopwatch();
            swTransit.Start();

            var status = new TextStatusNotifier("Checking...");
            _manager.Attach(_options.Collection, status);
            try
            {
                return await InternalTransit(dryRun, status, token);
            }
            catch (OperationCanceledException)
            {
                _logger.Warning("Collection {Database}.{Collection} transit was cancelled", _options.Database,
                    _options.Collection);
                
                // TODO: return process result
                return TransferResults.Empty;
            }
            catch (Exception e)
            {
                _logger.Error(e, "Failed to transit collection {Database}.{Collection}", _options.Database,
                    _options.Collection);
                throw;
            }
            finally
            {
                swTransit.Stop();
                _logger.Debug("Collection {Database}.{Collection} transit finished in {elapsed}", _options.Database,
                    _options.Collection, swTransit.Elapsed);
                _manager.Detach(_options.Collection);
            }
        }

        private async Task<TransferResults> InternalTransit(bool dryRun, TextStatusNotifier progress, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            
            var transitChannel = Channel.CreateBounded<List<ReplaceOneModel<BsonDocument>>>(_options.Workers);

            var (filter, count) = await _preparationHandler.PrepareCollectionAsync(_options.IterativeTransferOptions, progress, token);
            
            if (count == 0)
            {
                _logger.Information("Collection {Collection} is up-to date, skipping transit", _options.Collection);
                return TransferResults.Empty;
            }

            var notifier = new ProgressNotifier(count);
            _manager.Attach(_options.Collection, notifier);

            var writer = _documentsWriterFactory.Create(transitChannel, notifier, dryRun);

            var sw = new Stopwatch();
            sw.Start();

            var writeHandler = writer.WriteAsync(token);

            await _source.ReadDocumentsAsync(filter, transitChannel, _options.BatchSize,
                _options.FetchKeyFromDestination, _options.KeyFields ?? Array.Empty<string>(), _options.Upsert,
                _destination, token);

            var results = await writeHandler;
            sw.Stop();
            
            _logger.Debug("Transfer was completed in {Elapsed}", sw.Elapsed);

            _logger.Information("Transferred {S}; Retried {R}; Failed {F};", results.Processed, results.Retried,
                results.Failed);

            return results;
        }
    }
}