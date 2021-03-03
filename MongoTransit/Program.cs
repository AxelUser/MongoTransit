using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
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

                    var logPath = Path.Join(toolOpts.LogsDirectory ?? "", $"transit_{DateTime.Now:yyyy_MM_dd}.log");
                    var log = new LoggerConfiguration()
                        .MinimumLevel.Debug()
                        .Enrich.WithProperty("Scope", "Runner")
                        .WriteTo.Console(toolOpts.Verbose ? LogEventLevel.Debug : LogEventLevel.Information,
                            "[{Timestamp:HH:mm:ss} {Level:u3}][{Scope}] {Message:lj}{NewLine}{Exception}")
                        .WriteTo.File(logPath,
                            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}][{Scope}] {Message:lj}{NewLine}{Exception}")
                        .CreateLogger(); 
                    
                    var cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, _) =>
                    {
                        log.Debug("Cancellation was requested");
                        cts.Cancel();
                    };
                    
                    await TransitRunner.RunAsync(log, config, IterateCycles(toolOpts.Runs), toolOpts.DryRun, cts.Token);
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