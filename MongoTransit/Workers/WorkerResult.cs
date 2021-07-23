namespace MongoTransit.Workers
{
    public record WorkerResult(long Successful, long Retryable, long Failed);
}