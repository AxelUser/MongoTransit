namespace MongoTransit.Workers
{
    public record TransferResults(long Processed, long Retried, long Failed);
}