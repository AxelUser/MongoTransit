using System.Linq;
using System.Threading.Tasks;
using CommandLine;

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
                    await TransitRunner.RunAsync(config, default);
                });
        }
    }
}