using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Notifications;
using MongoTransit.Options;
using MongoTransit.Preparation;
using MongoTransit.Processing;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using Serilog;

namespace MongoTransit.Transit
{
    public static class TransitRunner
    {
        public static async Task RunAsync(ILogger logger, CollectionTransitOptions[] options,
            IEnumerable<int> cyclesIterator, bool dryRun, TimeSpan notificationInterval, CancellationToken token)
        {
            var handlers = new ICollectionTransitHandler?[options.Length];
            var operations = new Task[options.Length];

            var progressManager = new ProgressManager();
            await using var notification = new NotificationLoop(progressManager, logger, notificationInterval);
            
            foreach (var cycle in cyclesIterator)
            {
                token.ThrowIfCancellationRequested();
                
                logger.Information("Transition iteration #{Number}", cycle);
                
                for (var idx = 0; idx < options.Length; idx++)
                {
                    var currentOptions = options[idx];
                        
                    handlers[idx] = handlers[idx] == null ? CreateCollectionHandler(currentOptions, progressManager, logger) : handlers[idx];
                    var handler = handlers[idx];
                        
                    operations[idx] = Task.Run(async () =>
                    {
                        try
                        {
                            await handler!.TransitAsync(dryRun, token);
                        }
                        catch (Exception e)
                        {
                            logger.Error(e, "Failed to transit collection {Collection}", currentOptions.Collection);
                            throw;
                        }
                    }, token);
                }
             
                logger.Information("Started {N} parallel transit operations", operations.Length);
                await Task.WhenAll(operations);
            }
        }

        private static ICollectionTransitHandler CreateCollectionHandler(CollectionTransitOptions currentOptions,
            IProgressManager progressManager, ILogger logger)
        {
            var collectionLogger = logger.ForContext("Scope", currentOptions.Collection);

            var sourceFactory = new SourceRepositoryFactory(currentOptions.SourceConnectionString,
                currentOptions.Database, currentOptions.Collection);
            var destFactory = new DestinationRepositoryFactory(currentOptions.DestinationConnectionString,
                currentOptions.Database, currentOptions.Collection);
            var preparationHandler = new CollectionPreparationHandler(currentOptions.Collection,
                destFactory.Create(logger), sourceFactory.Create(logger), logger);

            var workersCount = currentOptions.Workers * Environment.ProcessorCount;
            var workerPoolFactory = new DocumentsWriterFactory(workersCount, workersCount, currentOptions.Collection,
                destFactory, logger);
            
            // ReSharper disable once ConstantNullCoalescingCondition
            var handler = new CollectionTransitHandler(sourceFactory, destFactory, preparationHandler,
                workerPoolFactory, progressManager,
                collectionLogger, currentOptions);
            return handler;
        }
    }
}