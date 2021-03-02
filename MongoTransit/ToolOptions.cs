using CommandLine;

namespace MongoTransit
{
    public class ToolOptions
    {
        [Option('c', "config", Required = true, HelpText = "YAML file with configuration")]
        public string ConfigFile { get; set; } = null!;

        [Option('r', "runs", Required = false, Default = 0, HelpText = "How many transition cycles should tool do")]
        public int Runs { get; set; }
    }
}