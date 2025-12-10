using System.Collections.Concurrent;

namespace LiveCoding
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting manual BoundedWorkQueue demo...");
            await Demo.RunAsync();
            Console.WriteLine("Done. Press any key to exit.");
            Console.ReadKey();
        }
    }

    /// <summary>
    /// Bounded concurrent work queue implemented using
    /// ConcurrentQueue + SemaphoreSlim (no Channels).
    /// Capacity here means: at most `capacity` work items are
    /// "in-flight" at any time (queued or currently running).
    /// </summary>
    public sealed class BoundedWorkQueue : IAsyncDisposable
    {
        private readonly ConcurrentQueue<Func<CancellationToken, Task>> _queue =
            new ConcurrentQueue<Func<CancellationToken, Task>>();

        // Number of work items currently queued (available for workers).
        private readonly SemaphoreSlim _items;

        // Remaining "capacity slots" for new work items.
        // This enforces the global bound (queued + running <= capacity).
        private readonly SemaphoreSlim _slots;

        private readonly CancellationTokenSource _shutdown = new();
        private readonly Task[] _workers;
        private volatile bool _accepting = true;

        public BoundedWorkQueue(int maxDegreeOfParallelism, int capacity)
        {
            if (maxDegreeOfParallelism <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));
            if (capacity <= 0)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            // Initially no items are queued.
            _items = new SemaphoreSlim(0, capacity);

            // Initially all capacity slots are free.
            _slots = new SemaphoreSlim(capacity, capacity);

            _workers = new Task[maxDegreeOfParallelism];
            for (int i = 0; i < _workers.Length; i++)
            {
                _workers[i] = Task.Run(() => WorkerLoopAsync(_shutdown.Token));
            }
        }

        /// <summary>
        /// Enqueue a work item. Blocks (asynchronously) when capacity is full.
        /// Throws OperationCanceledException if the caller cancels.
        /// Throws InvalidOperationException if the queue has been stopped.
        /// </summary>
        public async ValueTask EnqueueAsync(
            Func<CancellationToken, Task> workItem,
            CancellationToken cancellationToken = default)
        {
            if (workItem is null)
                throw new ArgumentNullException(nameof(workItem));

            // Fast rejection if shutdown already requested.
            if (!_accepting || _shutdown.IsCancellationRequested)
                throw new InvalidOperationException("The queue is no longer accepting work items.");

            // Wait for a free capacity slot. This enforces the global bound.
            await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);

            // After we acquired the slot, we might have raced with StopAsync.
            if (!_accepting || _shutdown.IsCancellationRequested)
            {
                _slots.Release(); // give the slot back
                throw new InvalidOperationException("The queue has been stopped.");
            }

            _queue.Enqueue(workItem);

            // Signal that a new item is available for workers.
            _items.Release();
        }

        /// <summary>
        /// Stops accepting new work, cancels in-flight work, and waits
        /// for worker tasks to exit.
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (!_accepting)
                return;

            _accepting = false;

            // Request cooperative cancellation for workers and work items.
            _shutdown.Cancel();

            var allWorkers = Task.WhenAll(_workers);

            if (allWorkers.IsCompleted)
            {
                await allWorkers.ConfigureAwait(false);
            }
            else
            {
                // .NET 6+ Task.WaitAsync
                await allWorkers.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task WorkerLoopAsync(CancellationToken shutdownToken)
        {
            try
            {
                while (true)
                {
                    // Wait for an available item to process.
                    await _items.WaitAsync(shutdownToken).ConfigureAwait(false);

                    if (shutdownToken.IsCancellationRequested)
                        break;

                    if (!_queue.TryDequeue(out var workItem))
                    {
                        // Should not happen; keep capacity accounting consistent.
                        _slots.Release();
                        continue;
                    }

                    try
                    {
                        // Run the work item with the shared shutdown token.
                        await workItem(shutdownToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
                    {
                        // Normal during shutdown.
                    }
                    catch (Exception ex)
                    {
                        // In a real system, log properly.
                        Console.Error.WriteLine($"Work item failed: {ex}");
                    }
                    finally
                    {
                        // Work item is finished (or faulted/canceled),
                        // free the capacity slot for another work item.
                        _slots.Release();
                    }
                }
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                // Normal exit on shutdown.
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
                _items.Dispose();
                _slots.Dispose();
            }
        }
    }

    public static class Demo
    {
        public static async Task RunAsync()
        {
            // maxDegreeOfParallelism = 4 workers
            // capacity = at most 10 work items in-flight at any time
            await using var queue = new BoundedWorkQueue(
                maxDegreeOfParallelism: 4,
                capacity: 10);

            using var cts = new CancellationTokenSource();

            var producers = new Task[30];

            for (int i = 0; i < producers.Length; i++)
            {
                int jobId = i;
                producers[i] = Task.Run(async () =>
                {
                    await queue.EnqueueAsync(async ct =>
                    {
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
