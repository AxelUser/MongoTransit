using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoTransit.Processing.Workers
{
    public interface IWriteWorkerFactory
    {
        Task<WorkerResult> RunWorker(ChannelWriter<ReplaceOneModel<BsonDocument>>? failedWrites,
            ILogger workerLogger,
            CancellationToken token);

        Task<WorkerResult> RunRetryWorker(ChannelReader<ReplaceOneModel<BsonDocument>> failedWrites,
            ILogger workerLogger,
            CancellationToken token);
    }
}