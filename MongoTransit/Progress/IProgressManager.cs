namespace MongoTransit.Progress
{
    public interface IProgressManager
    {
        void Attach(string name, IOperationStatusProvider provider);
        void Detach(string name);
        bool Available { get; }
        string GetStatus();
    }
}