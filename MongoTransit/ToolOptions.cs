using CommandLine;

namespace MongoTransit
{
    public class ToolOptions
    {
        [Option('c', "config", Required = true, HelpText = "YAML file with configuration")]
        public string ConfigFile { get; set; } = null!;

        [Option('r', "runs", Required = false, Default = 0, HelpText = "How many transition cycles should tool do. Zero value will result in infinite cycle.")]
        public int Runs { get; set; }

        [Option('v', "verbose", HelpText = "Log debug information into console")]
        public bool Verbose { get; set; }

        [Option('d', "dry", HelpText = "Run tool without inserting or updating records")]
        public bool DryRun { get; set; }

        [Option('l', "logs", HelpText = "Directory for storing logs")]
        public string LogsDirectory { get; set; }
    }
}