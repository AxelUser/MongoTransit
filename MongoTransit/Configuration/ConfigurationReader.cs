using System;
using System.Collections.Generic;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace MongoTransit.Configuration
{
    public static class ConfigurationReader
    {
        // ReSharper disable once ClassNeverInstantiated.Global
        public class CollectionOption
        {
            public string Name { get; set; }

            public string Database { get; set; }

            public string[]? KeyFields { get; set; }

            public bool FetchKeyFromDestination { get; set; }

            public bool NoUpsert { get; set; }

            public IterativeCollectionOptions? IterativeOptions { get; set; }
        }

        // ReSharper disable once ClassNeverInstantiated.Global
        public class IterativeCollectionOptions
        {
            public string Field { get; set; }

            public DateTime? ForcedCheckpoint { get; set; }
            
            public int OffsetInMinutes { get; set; }
        }

        public static IReadOnlyList<CollectionOption> Read(string file)
        {
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();

            using var reader = File.OpenText(file);
            return deserializer.Deserialize<CollectionOption[]>(reader);
        }
    }
}