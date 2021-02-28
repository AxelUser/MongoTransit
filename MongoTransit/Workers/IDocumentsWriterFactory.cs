using System.Collections.Generic;
using System.Threading.Channels;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Progress;

namespace MongoTransit.Workers
{
    public interface IDocumentsWriterFactory
    {
        IDocumentsWriter Create(ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader,
            IProgressNotifier notifier,
            bool upsert,
            bool dryRun);
    }
}