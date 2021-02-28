using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using YamlDotNet.Serialization;

namespace MongoTransit
{
    public class ConfigurationReader
    {
        private record Options(string ReplicaSet, string ShardedCluster, int Workers, int BatchSize,
            CollectionOption[] Collections);

        private record CollectionOption(string Name, string Database, string[] UpsertFields,
            IterativeCollectionOptions IterativeCollection);

        private record IterativeCollectionOptions(string Field, bool StopOnError, DateTime ForcedCheckpoint);

        public static IEnumerable<CollectionTransitOptions> Read(string file)
        {
            var deserializer = new DeserializerBuilder()
                .Build();

            using var reader = File.OpenText(file);
            var opts = deserializer.Deserialize<Options>(reader);

            foreach (var coll in opts.Collections)
            {
                var iterOpts = coll.IterativeCollection != null
                    ? new IterativeTransitOptions(coll.IterativeCollection.Field,
                        coll.IterativeCollection.StopOnError, coll.IterativeCollection.ForcedCheckpoint)
                    : null;

                yield return new CollectionTransitOptions(opts.ReplicaSet,
                    opts.ShardedCluster, coll.Database, coll.Name, coll.UpsertFields,
                    opts.Workers, opts.BatchSize, iterOpts);
            }
        }
    }
}