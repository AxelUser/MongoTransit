using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MongoTransit.Transit
{
    public static class TransitRunner
    {
        public static async Task RunAsync(CollectionTransitOptions[] options, CancellationToken token)
        {
            var handlers = new List<CollectionTransitHandler>(options.Length);
            var operations = new List<Task>(options.Length);
            
            // TODO pass number of runs into args
            while (token.IsCancellationRequested)
            {

                for (var idx = 0; idx < options.Length; idx++)
                {
                    handlers[idx] ??= new CollectionTransitHandler(options[idx]);
                    operations[idx] = handlers[idx].TransitAsync(token);
                }
                
                await Task.WhenAll(operations);
            }        
        }
    }
}