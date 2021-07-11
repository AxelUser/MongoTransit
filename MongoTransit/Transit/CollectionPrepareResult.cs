using MongoTransit.Storage.Source;

namespace MongoTransit.Transit
{
    public record CollectionPrepareResult(SourceFilter Filter, long Count);
}