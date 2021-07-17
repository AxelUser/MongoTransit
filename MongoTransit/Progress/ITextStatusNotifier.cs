namespace MongoTransit.Progress
{
    public interface ITextStatusNotifier : IOperationStatusProvider
    {
        new string Status { get; set; }
    }
}