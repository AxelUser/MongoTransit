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
                        .MinimumLevel.Debug()
                        .WriteTo.Console(toolOpts.Verbose ? LogEventLevel.Debug : LogEventLevel.Information,
                            "[{Timestamp:HH:mm:ss} {Level:u3}][{Collection}{Worker}] {Message:lj}{NewLine}{Exception}")
                        .CreateLogger();    
                    await TransitRunner.RunAsync(log, config, IterateCycles(toolOpts.Runs), toolOpts.DryRun, default);
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