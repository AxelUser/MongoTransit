using MongoTransit.Storage.Source;
using MongoTransit.Storage.Source.Models;

namespace MongoTransit.Preparation
{
    public record CollectionPrepareResult(SourceFilter Filter, long Count);
}