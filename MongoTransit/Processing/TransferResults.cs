namespace MongoTransit.Processing
{
    public record TransferResults(long Processed, long Retried, long Failed);
}