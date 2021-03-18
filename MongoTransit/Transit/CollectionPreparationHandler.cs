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
            progress.Status = "Removing documents from destination...";
            await _destination.DeleteAllDocumentsAsync(token);
            progress.Status = "Counting documents...";
            var count = await _source.CountAllDocumentsAsync(token);
            return new CollectionPrepareResult(new BsonDocument(), count);
        }

        private async Task<CollectionPrepareResult> CheckIterativeCollectionAsync(TextStatusProvider progress, IterativeTransitOptions iterOpts, CancellationToken token)
        {
            _logger.Debug("Detected iterative transit option. Fetching checkpoint and lag");

            var (checkpointField, offset, forcedCheckpoint) = iterOpts;

            progress.Status = "Searching checkpoint...";

            DateTime? lastCheckpoint;
            
            if (forcedCheckpoint != null)
            {
                _logger.Information("Forced to use checkpoint {ForcedCheckpoint} for collection {Collection}",
                    iterOpts.ForcedCheckpoint, _collectionName);
                lastCheckpoint = iterOpts.ForcedCheckpoint;
            }
            else
            {
                _logger.Debug("Fetching last checkpoint for collection {Collection}", _collectionName);
                lastCheckpoint = await _destination.FindLastCheckpointAsync(checkpointField, token);
                lastCheckpoint -= offset;
            }
            
            if (lastCheckpoint == null)
            {
                throw new Exception($"Couldn't get checkpoint for collection {_collectionName}");
            }

            _logger.Debug("Counting how many documents should be transferred");
            progress.Status = "Counting documents...";
            var count = await _source.CountLagAsync(checkpointField, lastCheckpoint.Value, token);

            _logger.Debug("Collection {Collection} has checkpoint {LastCheckpoint} and lag {Lag:N0}",
                _collectionName, lastCheckpoint, count);
            
            var filter = new BsonDocument(checkpointField, new BsonDocument("$gte", lastCheckpoint));

            return new CollectionPrepareResult(filter, count);
        }
    }
}