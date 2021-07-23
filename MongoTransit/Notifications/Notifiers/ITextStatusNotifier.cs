namespace MongoTransit.Notifications.Notifiers
{
    public interface ITextStatusNotifier : IOperationStatusProvider
    {
        new string Status { get; set; }
    }
}