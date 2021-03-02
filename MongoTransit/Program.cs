using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CommandLine;
using MongoTransit.Transit;
using Serilog;
using Serilog.Events;

namespace MongoTransit
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            await Parser.Default.ParseArguments<ToolOptions>(args)
                .WithParsedAsync(async toolOpts =>
                {
                    var config = ConfigurationReader.Read(toolOpts.ConfigFile).ToArray();

                    var log = new LoggerConfiguration()
                        .WriteTo.Console(LogEventLevel.Information)
                        .CreateLogger();
                    await TransitRunner.RunAsync(log, config, IterateCycles(toolOpts.Runs), default);
                });
        }

        private static IEnumerable<int> IterateCycles(int cycles)
        {
            var cycle = 1;
            if (cycles == 0)
            {
                while (true)
                {
                    yield return cycle++;
                }
            }

            while (cycle <= cycles)
            {
                yield return cycle++;
            }
        }
    }
}