﻿using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoTransit.Options;
using MongoTransit.Progress;
using MongoTransit.Storage.Destination;
using MongoTransit.Storage.Source;
using Serilog;
using Stopwatch = System.Diagnostics.Stopwatch;

namespace MongoTransit.Transit
{
    public class CollectionPreparationHandler : ICollectionPreparationHandler
    {
        private readonly string _collectionName;
        private readonly IDestinationRepository _destination;
        private readonly ISourceRepository _source;
        private readonly ILogger _logger;

        public CollectionPreparationHandler(string collectionName, IDestinationRepository destination, ISourceRepository source, ILogger logger)
        {
            _collectionName = collectionName;
            _destination = destination;
            _source = source;
            _logger = logger;
        }
        
        public async Task<CollectionPrepareResult> PrepareCollectionAsync(
            IterativeTransitOptions? iterativeTransitOptions,
            TextStatusProvider progress,
            CancellationToken token)
        {
            var sw = new Stopwatch();
            sw.Start();
            var result = iterativeTransitOptions != null
                ? await CheckIterativeCollectionAsync(progress, iterativeTransitOptions, token)
                : await CheckFullCollectionTransitAsync(progress, token);
            sw.Stop();
            _logger.Debug("Collection check was completed in {Elapsed} ms", sw.ElapsedMilliseconds);
            return result;
        }

        private async Task<CollectionPrepareResult> CheckFullCollectionTransitAsync(TextStatusProvider progress, CancellationToken token)
        {
            _logger.Debug("Detected full transit for collection {Collection}", _collectionName);
            progress.Status = "Removing documents from destination...";
            await _destination.DeleteAllDocumentsAsync(token);
            progress.Status = "Counting documents...";
            var filter = new BsonDocument();
            var count = await _source.CountLagAsync(filter, token);
            return new CollectionPrepareResult(filter, count);
        }

        private async Task<CollectionPrepareResult> CheckIterativeCollectionAsync(TextStatusProvider progress, IterativeTransitOptions iterOpts, CancellationToken token)
        {
            _logger.Debug("Detected iterative transit for collection {Collection} with checkpoint field {Field}",
                _collectionName, iterOpts.Field);
            progress.Status = "Searching checkpoint...";
            var filter = await CreateIterativeFilterAsync(iterOpts, token);

            _logger.Debug("Counting how many documents should be transferred");
            progress.Status = "Counting documents...";
            var count = await _source.CountLagAsync(filter, token);
            _logger.Debug("Collection {Collection} has lag {Lag:N0}", _collectionName, count);
            
            return new CollectionPrepareResult(filter, count);
        }

        private async Task<BsonDocument> CreateIterativeFilterAsync(IterativeTransitOptions iterOpts, CancellationToken token)
        {
            var (checkpointField, offset, forcedCheckpoint) = iterOpts;

            if (forcedCheckpoint != null)
            {
                _logger.Information("Forced to use checkpoint {ForcedCheckpoint} for collection {Collection}",
                    iterOpts.ForcedCheckpoint, _collectionName);
                return CreateFilterModel(iterOpts.ForcedCheckpoint);
            }

            _logger.Debug("Fetching last checkpoint for collection {Collection}", _collectionName);
            var lastCheckpoint = await _destination.FindLastCheckpointAsync(checkpointField, token);

            if (lastCheckpoint != null)
            {
                lastCheckpoint -= offset;
                _logger.Debug("Collection {Collection} will be transferred from checkpoint {LastCheckpoint}",
                    _collectionName, lastCheckpoint);
            }
            else
            {
                _logger.Warning("Collection {Collection} doesn't have checkpoint", _collectionName);
            }
                
            return CreateFilterModel(lastCheckpoint);

            BsonDocument CreateFilterModel(DateTime? checkpointValue)
            {
                return checkpointValue != null
                    ? new BsonDocument(checkpointField, new BsonDocument("$gte", checkpointValue.Value))
                    : new BsonDocument
                    {
                        [checkpointField] = new BsonDocument("$ne", BsonNull.Value)
                    };
            }
        }
    }
}