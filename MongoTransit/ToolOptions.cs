using CommandLine;

namespace MongoTransit
{
    public class ToolOptions
    {
#pragma warning disable 8618
        
        [Option('s', "source", Required = true, HelpText = "Connection string for source server or cluster")]
        public string SourceConnectionString { get; set; }
        
        [Option('d', "destination", Required = true, HelpText = "Connection string for destination server or cluster")]
        public string DestinationConnectionString { get; set; }
        
        [Option('c', "config", Required = true, HelpText = "YAML file with configuration")]
        public string ConfigFile { get; set; }
#pragma warning restore 8618

        [Option('r', "runs", Default = 0, HelpText = "How many transition cycles should tool do. Zero value will result in infinite cycle.")]
        public int Runs { get; set; }

        [Option("verbose", HelpText = "Log debug information into console")]
        public bool Verbose { get; set; }

        [Option("dry", HelpText = "Run tool without inserting or updating records")]
        public bool DryRun { get; set; }

        [Option("logs", HelpText = "Directory for storing logs")]
        public string? LogsDirectory { get; set; }

        [Option('n', "notify", Default = 3, HelpText = "Notification interval in seconds")]
        public int NotificationInterval { get; set; }

        [Option('w', "workers", Default = 4, HelpText = "Amount of insertion workers per each CPU (core)")]
        public int WorkersPerCpu { get; set; }

        [Option('b', "batchSize", Default = 1000, HelpText = "Batch size for insertion")]
        public int BatchSize { get; set; }
    }
}