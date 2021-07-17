namespace MongoTransit.Progress
{
    public class TextStatusNotifier: ITextStatusNotifier
    {
        public TextStatusNotifier(string initialStatus)
        {
            Status = initialStatus;
        }
        
        public string Status { get; set; }
    }
}