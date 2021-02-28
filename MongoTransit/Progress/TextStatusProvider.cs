namespace MongoTransit.Progress
{
    public class TextStatusProvider: IOperationStatusProvider
    {
        public TextStatusProvider(string initialStatus)
        {
            Status = initialStatus;
        }
        
        public string Status { get; set; }
    }
}