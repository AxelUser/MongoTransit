using System.Collections.Generic;
using System.Threading.Channels;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Progress;

namespace MongoTransit.Workers
{
    public interface IWorkerPoolFactory
    {
        IWorkerPool Create(ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader,
            ProgressNotifier notifier,
            bool upsert,
            bool dryRun);
    }
}