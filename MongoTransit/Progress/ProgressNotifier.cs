using System.Threading;

namespace MongoTransit.Progress
{
    public class ProgressNotifier
    {
        private readonly long _max;
        private long _current;

        public ProgressNotifier(long max)
        {
            _max = max;
            _current = 0;
        }
        
        public (long current, long max) Progress => (_current, _max);
        
        public void Notify(int processed)
        {
            Interlocked.Add(ref _current, processed);
        }
    }
}