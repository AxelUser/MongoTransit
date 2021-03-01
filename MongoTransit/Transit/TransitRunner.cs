using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace MongoTransit.Transit
{
    public static class TransitRunner
    {
        public static async Task RunAsync(ILogger logger, CollectionTransitOptions[] options, CancellationToken token)
        {
            var handlers = new List<CollectionTransitHandler>(options.Length);
            var operations = new List<Task>(options.Length);
            
            // TODO pass number of runs into args
            while (token.IsCancellationRequested)
            {
                for (var idx = 0; idx < options.Length; idx++)
                {
                    handlers[idx] ??= new CollectionTransitHandler(logger.ForContext("Collection", options[idx].Collection), options[idx]);
                    operations[idx] = handlers[idx].TransitAsync(token);
                }
             
                logger.Information("Started {n} parallel transit operations", operations.Count);
                await Task.WhenAll(operations);
            }        
        }
    }
}