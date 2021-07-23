namespace MongoTransit.Processing.Workers
{
    public record WorkerResult(long Successful, long Retryable, long Failed);
}