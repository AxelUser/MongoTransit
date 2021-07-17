namespace MongoTransit.Progress
{
    public class TextStatusProvider: ITextStatusNotifier
    {
        public TextStatusProvider(string initialStatus)
        {
            Status = initialStatus;
        }
        
        public string Status { get; set; }
    }
}