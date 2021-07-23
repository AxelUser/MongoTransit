using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Options;
using MongoTransit.Preparation;
using MongoTransit.Processing;
using MongoTransit.Progress;
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

            var progressNotification = StartNotifier(logger, notificationInterval);
            try
            {
                foreach (var cycle in cyclesIterator)
                {
                    token.ThrowIfCancellationRequested();
                
                    logger.Information("Transition iteration #{Number}", cycle);
                
                    for (var idx = 0; idx < options.Length; idx++)
                    {
                        var currentOptions = options[idx];
                        
                        handlers[idx] = handlers[idx] == null ? CreateCollectionHandler(currentOptions, progressNotification, logger) : handlers[idx];
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
            finally
            {
                await StopNotifier(progressNotification);
            }
        }

        private static ICollectionTransitHandler CreateCollectionHandler(CollectionTransitOptions currentOptions,
            NotificationLoop progressNotification, ILogger logger)
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
                workerPoolFactory, progressNotification.Manager,
                collectionLogger, currentOptions);
            return handler;
        }

        private record NotificationLoop(Task Loop, ProgressManager Manager, CancellationTokenSource Cancellation);

        private static NotificationLoop StartNotifier(ILogger logger, TimeSpan delay)
        {
            var manager = new ProgressManager();
            var cts = new CancellationTokenSource();
            var loop = Task.Run(async () =>
            {
                logger.Debug("Started notification loop");
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        await Task.Delay(delay, cts.Token);
                        if (manager.Available)
                        {
                            logger.Information("Progress report:\n{Progress}", manager.GetStatus());    
                        }
                    }
                }
                finally
                {
                    logger.Debug("Stopped notification loop");
                }
            }, cts.Token);

            return new NotificationLoop(loop, manager, cts);
        }

        private static async Task StopNotifier(NotificationLoop loop)
        {
            var (task, _, cancellationTokenSource) = loop;
            cancellationTokenSource.Cancel();
            try
            {
                await task;
            }
            catch
            {
                // No matter what happened
            }
        }
    }
}