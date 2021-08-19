using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoTransit.Extensions;
using MongoTransit.Storage.Destination.Exceptions;
using Serilog;

namespace MongoTransit.Storage.Destination
{
    public class DestinationRepository: IDestinationRepository
    {
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly ILogger _logger;

        public DestinationRepository(IMongoCollection<BsonDocument> collection, ILogger logger)
        {
            _collection = collection;
            _logger = logger;
        }

        public async Task ReplaceManyAsync(List<ReplaceOneModel<BsonDocument>>? replacements, CancellationToken token)
        {
            if (replacements == null || !replacements.Any())
            {
                _logger.Debug("Empty bulk, skipping");
                return;
            }
            
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                await _collection.BulkWriteAsync(replacements, new BulkWriteOptions
                {
                    IsOrdered = false,
                    BypassDocumentValidation = true
                }, token);
                sw.Stop();

                _logger.Debug("Successfully processed bulk of {Count:N0} documents in {Elapsed:N1} ms",
                    replacements.Count, sw.ElapsedMilliseconds);
            }
            catch (MongoBulkWriteException<BsonDocument> bwe)
            {
                sw.Stop();
                var errors = bwe.WriteErrors.Select(error => new ReplaceErrorInfo(error.Index, error.Message)).ToList();
                _logger.Debug("Processed bulk of {Count:N0} documents with {Errors} errors in {Elapsed:N1} ms",
                    replacements.Count, errors.Count, sw.ElapsedMilliseconds);
                throw new ReplaceManyException(errors, bwe.Result.ProcessedRequests.Count);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                sw.Stop();
                _logger.Debug("Failed to process bulk of {Count:N0} documents errors in {Elapsed:N1} ms",
                    replacements.Count, sw.ElapsedMilliseconds);
                throw new ReplaceManyException(e);
            }

        }

        public async Task ReplaceDocumentAsync(ReplaceOneModel<BsonDocument> model, CancellationToken token)
        {
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                var result = await _collection.ReplaceOneAsync(model.Filter, model.Replacement, new ReplaceOptions
                {
                    BypassDocumentValidation = true
                }, token);
                sw.Stop();

                _logger.Debug(
                    result.ModifiedCount == 1
                        ? "Successfully retried replacement of document (ID: {Id}) in {Elapsed:N1} ms"
                        : "Failed replacement of document (ID: {Id}) in {Elapsed:N1} ms. Document is missing",
                    model.Replacement["_id"], sw.ElapsedMilliseconds);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new ReplaceOneException($"Failed to replace document: {e.Message}", e);
            }

        }

        public async Task DeleteAllDocumentsAsync(CancellationToken token)
        {
            var sw = new Stopwatch();
            sw.Start();
            await _collection.DeleteManyAsync(new BsonDocument(), token);
            sw.Stop();
            _logger.Debug("Removed all documents at collection in {Elapsed}", sw.Elapsed);
        }
        
        public async Task<DateTime?> FindLastCheckpointAsync(string checkpointField, CancellationToken token)
        {
            var checkpointBson = await (await _collection.FindAsync(new BsonDocument
            {
                [checkpointField] = new BsonDocument
                {
                    ["$exists"] = true,
                    ["$ne"] = BsonNull.Value
                }
            }, new FindOptions<BsonDocument>
            {
                Sort = new BsonDocument(checkpointField, -1),
                Limit = 1,
                Projection = new BsonDocument
                {
                    ["_id"] = true,
                    [checkpointField] = true
                }
            }, token)).SingleOrDefaultAsync(token);

            if (checkpointBson == null)
            {
                return null;
            }

            return checkpointBson[checkpointField].ToUniversalTime();
        }

        public async Task<BsonDocument?> FindDocumentAsync(BsonDocument document, CancellationToken token)
        {
            var cursor = await _collection.FindAsync( document.GetFilterBy("_id"),
                cancellationToken: token);
            return await cursor.SingleOrDefaultAsync(token);
        }
    }
}