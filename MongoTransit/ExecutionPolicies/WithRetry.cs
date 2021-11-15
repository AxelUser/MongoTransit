using System;
using System.Threading;
using System.Threading.Tasks;

namespace MongoTransit.ExecutionPolicies;

public static class WithRetry
{
    public static async Task<TResult> ExecuteAsync<TResult>(int attempts, TimeSpan cooldown, Func<Task<TResult>> wrappedFunc, Func<Exception, bool> shouldRetry, CancellationToken ct, Action<Exception>? onRetry = null)
    {
        if (attempts <= 0) throw new ArgumentException("Should be greater than zero", nameof(attempts));

        while (attempts > 0)
        {
            attempts--;
            try
            {
                return await wrappedFunc();
            }
            catch (Exception e)
            {
                if (attempts == 0 || !shouldRetry(e)) throw;
                onRetry?.Invoke(e);
                await Task.Delay(cooldown, ct);
            }
        }

        throw new InvalidOperationException($"Exceeded retry attempts of {attempts}");
    }
}