using System;
using MongoDB.Bson;

namespace MongoTransit.Storage.Source
{
    public record SourceFilter(string? Field, DateTime? Checkpoint)
    {
        public BsonDocument ToBsonDocument()
        {
            if (string.IsNullOrWhiteSpace(Field))
            {
                return new BsonDocument();
            } 
            return Checkpoint != null
                ? new BsonDocument(Field, new BsonDocument("$gte", Checkpoint.Value))
                : new BsonDocument
                {
                    [Field] = new BsonDocument("$ne", BsonNull.Value)
                };
        }

        public static SourceFilter Empty = new(null, null);
    }
}