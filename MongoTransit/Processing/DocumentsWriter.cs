using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Processing.Workers;
using Serilog;

namespace MongoTransit.Processing
{
    public class DocumentsWriter : IDocumentsWriter
    {
        private readonly ILogger _logger;
        private readonly Channel<ReplaceOneModel<BsonDocument>> _retriesChannel;
        private readonly Task<WorkerResult>[] _insertionWorkers;
        private readonly Task<WorkerResult>[] _retryWorkers;
        private readonly string _collectionName;
        private readonly IWriteWorkerFactory _writeWorkerFactory;

        public DocumentsWriter(int insertionWorkersCount,
            int retryWorkersCount,
            string collectionName,
            IWriteWorkerFactory writeWorkerFactory,
            ILogger logger)
        {
            _collectionName = collectionName;
            _writeWorkerFactory = writeWorkerFactory;
            _logger = logger;
            _insertionWorkers = insertionWorkersCount > 0
                ? new Task<WorkerResult>[insertionWorkersCount]
                : throw new ArgumentOutOfRangeException(nameof(insertionWorkersCount), insertionWorkersCount, "Should be greater than zero");
            _retryWorkers = retryWorkersCount >= 0
                ? new Task<WorkerResult>[retryWorkersCount]
                : throw new ArgumentOutOfRangeException(nameof(insertionWorkersCount), insertionWorkersCount, "Should be positive");
            
            _retriesChannel = Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>();
        }

        public async Task<TransferResults> WriteAsync(CancellationToken token)
        {
            StartWorkers(token);
            return await WaitUntilFinishedAsync();
        }

        private void StartWorkers(CancellationToken token)
        {
            for (var workerN = 0; workerN < _insertionWorkers.Length; workerN++)
            {
                var workerLogger = _logger.ForContext("Scope", $"{_collectionName}-{workerN:00}");
                _insertionWorkers[workerN] = _writeWorkerFactory.RunWorker(_retriesChannel.Writer,
                    workerLogger, token);
            }

            for (var retryWorkerN = 0; retryWorkerN < _retryWorkers.Length; retryWorkerN++)
            {
                var retryLogger = _logger.ForContext("Scope", $"{_collectionName}-Retry{retryWorkerN:00}");
                _retryWorkers[retryWorkerN] = _writeWorkerFactory.RunRetryWorker(_retriesChannel.Reader,
                    retryLogger, token);
            }

            _logger.Debug("Started {I:N0} insertion worker(s) and {R:0} retry worker(s)", _insertionWorkers.Length, _retryWorkers.Length);
        }

        private async Task<TransferResults> WaitUntilFinishedAsync()
        {
            _logger.Debug("Waiting for insertion-workers to finish");
            await Task.WhenAll(_insertionWorkers);
            
            _logger.Debug("Insertion-workers finished, waiting for retry-workers");
            _retriesChannel.Writer.Complete();
            await Task.WhenAll(_retryWorkers);

            return await GetResultsAsync();
        }

        private async Task<TransferResults> GetResultsAsync()
        {
            var processed = 0L;
            var retried = 0L;
            var failed = 0L;

            foreach (var worker in _insertionWorkers.Concat(_retryWorkers))
            {
                var (s, r, f) = await worker;
                processed += s;
                retried += r;
                failed += f;
            }

            return new TransferResults(processed, retried, failed);
        }
    }
}