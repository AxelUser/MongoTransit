using System.Collections.Generic;
using System.Threading.Channels;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Notifications.Notifiers;

namespace MongoTransit.Processing
{
    public interface IDocumentsWriterFactory
    {
        IDocumentsWriter Create(ChannelReader<List<ReplaceOneModel<BsonDocument>>> batchReader,
            IProgressNotifier notifier,
            bool dryRun);
    }
}