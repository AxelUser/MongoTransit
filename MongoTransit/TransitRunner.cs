using System.Collections.Generic;
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
            // TODO pass number of runs into args
            while (token.IsCancellationRequested)
            {
                var options = ConfigurationReader.Read(_configPath);
                var operations = new List<Task>();
                foreach (var option in options)
                {
                    operations.Add(CollectionTransitHandler.RunAsync(option, token));
                }

                await Task.WhenAll(operations);
            }        
        }
    }
}