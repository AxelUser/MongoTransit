using System.Threading;

namespace MongoTransit.Progress
{
    public class ProgressNotifier: IOperationStatusProvider, IProgressNotifier
    {
        private readonly long _max;
        private long _current;

        public ProgressNotifier(long max)
        {
            _max = max;
            _current = 0;
        }

        public void Notify(long processed)
        {
            Interlocked.Add(ref _current, processed);
        }

        public string Status
        {
            get
            {
                var localCurrent = _current;
                var percentage = (double)localCurrent / _max;
                return $"{percentage:P1} ( {localCurrent:N0} / {_max:N0} )";
            }
        }
    }
}