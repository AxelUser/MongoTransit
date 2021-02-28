using System.Threading.Tasks;
using CommandLine;

namespace MongoTransit
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            await Parser.Default.ParseArguments<ToolOptions>(args)
                .WithParsedAsync(async options =>
                {
                    var runner = new TransitRunner(options.ConfigFile);
                    await runner.RunAsync(default);
                });
        }
    }
}