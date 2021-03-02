using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoTransit.Progress;
using Serilog;

namespace MongoTransit.Transit
{
    public static class TransitRunner
    {
        public static async Task RunAsync(ILogger logger, CollectionTransitOptions[] options,
            IEnumerable<int> cyclesIterator, CancellationToken token)
        {
            var handlers = new List<CollectionTransitHandler>(options.Length);
            var operations = new List<Task>(options.Length);

            var progressNotification = StartNotifier(logger, TimeSpan.FromSeconds(3));
            try
            {
                foreach (var cycle in cyclesIterator)
                {
                    token.ThrowIfCancellationRequested();
                
                    logger.Information("Transition iteration #{number}", cycle);
                
                    for (var idx = 0; idx < options.Length; idx++)
                    {
                        handlers[idx] ??= new CollectionTransitHandler(progressNotification.Manager,
                            logger.ForContext("Collection", options[idx].Collection), options[idx]);
                        operations[idx] = handlers[idx].TransitAsync(token);
                    }
             
                    logger.Information("Started {n} parallel transit operations", operations.Count);
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
            var loop = Task.Run(() =>
            {
                logger.Debug("Started notification loop");
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        Task.Delay(delay, cts.Token);
                        if (manager.Available)
                        {
                            logger.Information("Progress report:\n{progress}", manager.ToString());    
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
            await task;
        }
    }
}