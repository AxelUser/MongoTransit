using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;

namespace MongoTransit.Notifications
{
    public class ProgressManager : IProgressManager
    {
        private readonly ConcurrentDictionary<string, IOperationStatusProvider> _providers;
        private int _maxNameLength;

        public ProgressManager()
        {
            _providers = new ConcurrentDictionary<string, IOperationStatusProvider>();
            _maxNameLength = 0;
        }

        public void Attach(string name, IOperationStatusProvider provider)
        {
            _providers.AddOrUpdate(name, provider, (_, _) => provider);

            while (true)
            {
                var localLenght = _maxNameLength;
                if (localLenght >= name.Length) return;
                if (Interlocked.CompareExchange(ref _maxNameLength, name.Length, localLenght) == localLenght)
                    return;
            }
        }

        public void Detach(string name)
        {
            _providers.TryRemove(name, out _);
        }

        public bool Available => _providers.Any();

        public string GetStatus()
        {
            var sb = new StringBuilder();
            foreach (var (name, notifier) in _providers)
            {
                sb.AppendLine($"{name.PadRight(_maxNameLength)}: {notifier.Status}");
            }

            return sb.ToString();
        }
    }
}