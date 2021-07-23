using System;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace MongoTransit.Notifications
{
    public class NotificationLoop: IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task _loop;

        public NotificationLoop(IProgressManager manager, ILogger logger, TimeSpan delay)
        {
            _cts = new CancellationTokenSource();
            _loop = Task.Run(async () =>
            {
                logger.Debug("Started notification loop");
                try
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        await Task.Delay(delay, _cts.Token);
                        if (manager.Available)
                        {
                            logger.Information("Progress report:\n{Progress}", manager.GetStatus());    
                        }
                    }
                }
                finally
                {
                    logger.Debug("Stopped notification loop");
                }
            }, _cts.Token);
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            try
            {
                await _loop;
            }
            catch
            {
                // No matter what happened
            }
        }
    }
}