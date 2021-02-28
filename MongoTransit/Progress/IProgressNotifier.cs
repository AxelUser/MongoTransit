namespace MongoTransit.Progress
{
    public interface IProgressNotifier
    {
        void Notify(long processed);
    }
}