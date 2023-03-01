using System;
using System.Collections.Generic;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace MongoTransit
{
    public class ConfigurationReader
    {
        public class CollectionOption
        {
#pragma warning disable CS8618
            public string Name { get; set; }
#pragma warning restore CS8618

#pragma warning disable CS8618
            public string Database { get; set; }
#pragma warning restore CS8618

            public ShardedKey? ShardedKey { get; set; }

            public bool NoUpsert { get; set; }

            public IterativeCollectionOptions? IterativeOptions { get; set; }
        }
        
        public class ShardedKey
        {
#pragma warning disable CS8618
            public string[] Fields { get; set; }
#pragma warning restore CS8618

            public bool FromDestination { get; set; }
        }

        public class IterativeCollectionOptions
        {
#pragma warning disable CS8618
            public string Field { get; set; }
#pragma warning restore CS8618

            public DateTime? ForcedCheckpoint { get; set; }
            
            public int OffsetInMinutes { get; set; }
        }

        public static IEnumerable<CollectionOption> Read(string file)
        {
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();

            using var reader = File.OpenText(file);
            return deserializer.Deserialize<CollectionOption[]>(reader);

        }
    }
}