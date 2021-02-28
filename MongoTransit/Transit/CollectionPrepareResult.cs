using MongoDB.Bson;

namespace MongoTransit.Transit
{
    public record CollectionPrepareResult(BsonDocument Filter, long Count);
}