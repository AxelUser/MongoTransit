using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MongoTransit
{
    public class TransitRunner
    {
        private readonly string _configPath;

        public TransitRunner(string configPath)
        {
            _configPath = configPath;
        }

        public async Task RunAsync(CancellationToken token)
        {
            var options = ConfigurationReader.Read(_configPath).ToArray();

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