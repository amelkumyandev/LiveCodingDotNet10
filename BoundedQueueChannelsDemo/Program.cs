using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace LiveCoding
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting BoundedWorkQueue demo...");
            await Demo.RunAsync();
            Console.WriteLine("Done. Press any key to exit.");
            Console.ReadKey();
        }
    }

    public sealed class BoundedWorkQueue : IAsyncDisposable
    {
        private readonly Channel<Func<CancellationToken, Task>> _channel;
        private readonly CancellationTokenSource _shutdown = new();
        private readonly Task[] _workers;
        private volatile bool _accepting = true;

        public BoundedWorkQueue(int maxDegreeOfParallelism, int capacity)
        {
            if (maxDegreeOfParallelism <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));
            if (capacity <= 0)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            var options = new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            };

            _channel = Channel.CreateBounded<Func<CancellationToken, Task>>(options);

            _workers = new Task[maxDegreeOfParallelism];
            for (int i = 0; i < _workers.Length; i++)
            {
                _workers[i] = Task.Run(() => WorkerLoopAsync(_shutdown.Token));
            }
        }

        public async ValueTask EnqueueAsync(
            Func<CancellationToken, Task> workItem,
            CancellationToken cancellationToken = default)
        {
            if (workItem is null)
                throw new ArgumentNullException(nameof(workItem));

            if (!_accepting)
                throw new InvalidOperationException("The queue is no longer accepting work items.");

            try
            {
                await _channel.Writer
                    .WriteAsync(workItem, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (ChannelClosedException ex)
            {
                throw new InvalidOperationException("The queue has been stopped.", ex);
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (!_accepting)
                return;

            _accepting = false;

            // 1. Stop accepting new writes.
            _channel.Writer.TryComplete();

            // 2. Request cooperative cancellation for in-flight work.
            _shutdown.Cancel();

            // 3. Wait for all workers to finish or for external cancellation.
            var allWorkers = Task.WhenAll(_workers);

            if (allWorkers.IsCompleted)
            {
                await allWorkers.ConfigureAwait(false);
            }
            else
            {
                // Requires .NET 6+ for Task.WaitAsync
                await allWorkers
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private async Task WorkerLoopAsync(CancellationToken shutdownToken)
        {
            try
            {
                while (await _channel.Reader
                           .WaitToReadAsync(shutdownToken)
                           .ConfigureAwait(false))
                {
                    while (_channel.Reader.TryRead(out var workItem))
                    {
                        try
                        {
                            await workItem(shutdownToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
                        {
                            // Cooperative shutdown: the workItem observed cancellation.
                            return;
                        }
                        catch (Exception ex)
                        {
                            // In a real app, log instead of Console.
                            Console.Error.WriteLine($"Work item failed: {ex}");
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                // Normal on shutdown.
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await StopAsync().ConfigureAwait(false);
            }
            finally
            {
                _shutdown.Dispose();
            }
        }
    }

    public static class Demo
    {
        public static async Task RunAsync()
        {
            await using var queue = new BoundedWorkQueue(
                maxDegreeOfParallelism: 4,
                capacity: 10);

            using var cts = new CancellationTokenSource();

            // Simulate 30 work items submitted from “different” threads.
            var producers = new Task[30];
            for (int i = 0; i < producers.Length; i++)
            {
                int jobId = i;
                producers[i] = Task.Run(async () =>
                {
                    await queue.EnqueueAsync(async ct =>
                    {
                        // Simulate some async work
                        await Task.Delay(TimeSpan.FromMilliseconds(200), ct);
                        Console.WriteLine(
                            $"Job {jobId:00} handled on Thread {Environment.CurrentManagedThreadId}");
                    }, cts.Token);
                });
            }

            await Task.WhenAll(producers);

            Console.WriteLine("All jobs enqueued. Stopping queue...");

            await queue.StopAsync();

            Console.WriteLine("Queue stopped.");
        }
    }
}
