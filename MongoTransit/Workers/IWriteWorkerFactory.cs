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
        Task<(long processed, long totalRetried, long failed)> RunWorker(
            ChannelWriter<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token);

        Task<(long processed, long totalRetried, long failed)> RunRetryWorker(
            ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token);
    }
}