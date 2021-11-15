using System;
using System.Collections.Generic;
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
        private readonly int _insertionWorkersCount;
        private readonly int _retryWorkersCount;
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
            _insertionWorkersCount = insertionWorkersCount > 0
                ? insertionWorkersCount
                : throw new ArgumentOutOfRangeException(nameof(insertionWorkersCount), insertionWorkersCount, "Should be greater than zero");
            _retryWorkersCount = retryWorkersCount >= 0
                ? retryWorkersCount
                : throw new ArgumentOutOfRangeException(nameof(retryWorkersCount), retryWorkersCount, "Should be positive");
        }

        public async Task<TransferResults> WriteAsync(CancellationToken token)
        {
            var insertionWorkers = new Task<WorkerResult>[_insertionWorkersCount];   
            var retryWorkers = new Task<WorkerResult>[_retryWorkersCount];
            var retriesChannel = retryWorkers.Length > 0
                ? Channel.CreateUnbounded<ReplaceOneModel<BsonDocument>>()
                : null;

            if (retriesChannel != null)
            {
                for (var retryWorkerN = 0; retryWorkerN < retryWorkers.Length; retryWorkerN++)
                {
                    var retryLogger = _logger.ForContext("Scope", $"{_collectionName}-Retry{retryWorkerN:00}");
                    retryWorkers[retryWorkerN] = _writeWorkerFactory.RunRetryWorker(retriesChannel.Reader,
                        retryLogger, token);
                }
            }

            for (var workerN = 0; workerN < insertionWorkers.Length; workerN++)
            {
                var workerLogger = _logger.ForContext("Scope", $"{_collectionName}-{workerN:00}");
                insertionWorkers[workerN] = _writeWorkerFactory.RunWorker(retriesChannel?.Writer,
                    workerLogger, token);
            }

            _logger.Debug("Started {I:N0} insertion worker(s) and {R:0} retry worker(s)", insertionWorkers.Length, retryWorkers.Length);
            
            _logger.Debug("Waiting for insertion-workers to finish");
            await Task.WhenAll(insertionWorkers);

            if (retriesChannel != null)
            {
                _logger.Debug("Waiting for retry-workers to finish");
                retriesChannel.Writer.Complete();
                await Task.WhenAll(retryWorkers);
            }
            
            return await GetResultsAsync(insertionWorkers, retryWorkers);
        }

        private static async Task<TransferResults> GetResultsAsync(IEnumerable<Task<WorkerResult>> insertionWorkers,
            IEnumerable<Task<WorkerResult>> retryWorkers)
        {
            var processed = 0L;
            var retried = 0L;
            var failed = 0L;

            foreach (var worker in insertionWorkers)
            {
                var (s, f) = await worker;
                processed += s;
                failed += f;
            }
            
            foreach (var worker in retryWorkers)
            {
                var (s, f) = await worker;
                retried += s;
                failed += f;
            }

            return new TransferResults(processed, retried, failed);
        }
    }
}