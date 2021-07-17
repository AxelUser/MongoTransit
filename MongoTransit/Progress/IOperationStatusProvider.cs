namespace MongoTransit.Progress
{
    public interface IOperationStatusProvider
    {
        public string Status { get; }
    }
}