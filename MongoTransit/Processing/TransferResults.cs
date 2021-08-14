namespace MongoTransit.Processing
{
    public record TransferResults(long Processed, long Retried, long Failed)
    {
        public static TransferResults Empty => new TransferResults(0, 0, 0);

        public static TransferResults operator +(TransferResults a, TransferResults b)
        {
            return new TransferResults(a.Processed + b.Processed, a.Retried + b.Retried, a.Failed + b.Failed);
        }
    }
}