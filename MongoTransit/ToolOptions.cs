using CommandLine;

namespace MongoTransit
{
    public class ToolOptions
    {
        [Option('c', "config", Required = true, HelpText = "YAML file with configuration")]
        public string ConfigFile { get; set; }
    }
}