using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Options;
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
            var handlers = new CollectionTransitHandler[options.Length];
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
                        
                        var sourceFactory = new SourceRepositoryFactory(currentOptions.SourceConnectionString,
                            currentOptions.Database, currentOptions.Collection);
                        var destFactory = new DestinationRepositoryFactory(currentOptions.DestinationConnectionString,
                            currentOptions.Database, currentOptions.Collection);
                        
                        // ReSharper disable once ConstantNullCoalescingCondition
                        handlers[idx] ??= new CollectionTransitHandler(sourceFactory, destFactory, progressNotification.Manager,
                            logger.ForContext("Scope", currentOptions.Collection), currentOptions);

                        var currentHandler = handlers[idx];

                        operations[idx] = Task.Run(async () =>
                        {
                            try
                            {
                                await currentHandler.TransitAsync(dryRun, token);
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
                            logger.Information("Progress report:\n{Progress}", manager.ToString());    
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