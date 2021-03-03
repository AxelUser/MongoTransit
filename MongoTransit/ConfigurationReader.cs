using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using MongoTransit.Transit;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace MongoTransit
{
    public class ConfigurationReader
    {
        private class Options
        {
            public string From { get; set; }

            public string To { get; set; }

            public int Workers { get; set; }

            public int BatchSize { get; set; }

            public CollectionOption[] Collections { get; set; }
        }

        private class CollectionOption
        {
            public string Name { get; set; }

            public string Database { get; set; }

            public string[] UpsertFields { get; set; }

            public IterativeCollectionOptions? IterativeOptions { get; set; }
        }

        private class IterativeCollectionOptions
        {
            public string Field { get; set; }

            public DateTime? ForcedCheckpoint { get; set; }
        }

        public static IEnumerable<CollectionTransitOptions> Read(string file)
        {
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();

            using var reader = File.OpenText(file);
            var opts = deserializer.Deserialize<Options>(reader);

            foreach (var coll in opts.Collections)
            {
                var iterOpts = coll.IterativeOptions != null
                    ? new IterativeTransitOptions(coll.IterativeOptions.Field, coll.IterativeOptions.ForcedCheckpoint)
                    : null;

                yield return new CollectionTransitOptions(opts.From,
                    opts.To, coll.Database, coll.Name, coll.UpsertFields,
                    opts.Workers, opts.BatchSize, iterOpts);
            }
        }
    }
}