namespace MongoTransit.Notifications
{
    public interface IOperationStatusProvider
    {
        public string Status { get; }
    }
}