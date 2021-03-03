using System.Collections.Concurrent;
using System.Linq;
using System.Text;

namespace MongoTransit.Progress
{
    public class ProgressManager
    {
        private readonly ConcurrentDictionary<string, ProgressNotifier> _notifiers;

        public ProgressManager()
        {
            _notifiers = new ConcurrentDictionary<string, ProgressNotifier>();
        }

        public void Attach(string name, ProgressNotifier notifier)
        {
            _notifiers.AddOrUpdate(name, notifier, (_, _) => notifier);
        }

        public void Detach(string name)
        {
            _notifiers.TryRemove(name, out _);
        }

        public bool Available => _notifiers.Any();

        public override string ToString()
        {
            var sb = new StringBuilder();
            foreach (var (name, notifier) in _notifiers)
            {
                var (current, max) = notifier.Progress;
                var percentage = (double)current / max;
                sb.AppendLine($"{name}: {percentage:P1} ( {current:N0} / {max:N0} )");
            }

            return sb.ToString();
        }
    }
}