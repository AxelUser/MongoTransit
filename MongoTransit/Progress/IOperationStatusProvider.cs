namespace MongoTransit.Progress
{
    public interface IOperationStatusProvider
    {
        string Status { get; }
    }
}