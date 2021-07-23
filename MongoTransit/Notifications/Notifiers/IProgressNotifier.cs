namespace MongoTransit.Notifications.Notifiers
{
    public interface IProgressNotifier
    {
        void Notify(long processed);
    }
}