using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Workers
{
    public interface IWriteWorkerFactory
    {
        Task<(long processed, long totalRetried, long failed)> CreateWorker(
            ChannelWriter<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token);

        Task<(long processed, long totalRetried, long failed)> CreateRetryWorker(
            ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token);
    }
}