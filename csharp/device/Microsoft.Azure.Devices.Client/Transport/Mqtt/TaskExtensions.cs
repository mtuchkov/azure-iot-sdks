// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    static class TaskExtensions
    {
        public static Task WithTimeout(this Task task, TimeSpan timeout, Func<string> errorMessage)
        {
            return WithTimeout(task, timeout, errorMessage, CancellationToken.None);
        }

        public static Task WithTimeout(this Task task, TimeSpan timeout, Func<string> errorMessage, CancellationToken token)
        {
            timeout = NormalizeTimeout(timeout);

            if (task.IsCompleted || (timeout == Timeout.InfiniteTimeSpan && !token.CanBeCanceled))
            {
                return task;
            }

            return WithTimeoutInternal(task, timeout, errorMessage, token);
        }

        static async Task WithTimeoutInternal(Task task, TimeSpan timeout, Func<string> errorMessage, CancellationToken token)
        {
            using (CancellationTokenSource cts = CreateLinkedCancellationTokenSource(token))
            {
                if (task == await Task.WhenAny(task, Task.Delay(timeout, cts.Token)))
                {
                    cts.Cancel();
                    await task;
                    return;
                }
            }

            if (token.IsCancellationRequested)
            {
                task.IgnoreFault();
                token.ThrowIfCancellationRequested();
            }

            throw new TimeoutException(errorMessage());
        }

        public static Task<T> WithTimeout<T>(this Task<T> task, TimeSpan timeout, Func<string> errorMessage)
        {
            return WithTimeout(task, timeout, errorMessage, CancellationToken.None);
        }

        public static Task<T> WithTimeout<T>(this Task<T> task, TimeSpan timeout, Func<string> errorMessage, CancellationToken token)
        {
            timeout = NormalizeTimeout(timeout);

            if (task.IsCompleted || (timeout == Timeout.InfiniteTimeSpan && !token.CanBeCanceled))
            {
                return task;
            }

            return WithTimeoutInternal(task, timeout, errorMessage, token);
        }

        static async Task<T> WithTimeoutInternal<T>(Task<T> task, TimeSpan timeout, Func<string> errorMessage, CancellationToken token)
        {
            using (CancellationTokenSource cts = CreateLinkedCancellationTokenSource(token))
            {
                if (task == await Task.WhenAny(task, Task.Delay(timeout, cts.Token)))
                {
                    cts.Cancel();
                    return await task;
                }
            }

            if (token.IsCancellationRequested)
            {
                task.IgnoreFault();
                token.ThrowIfCancellationRequested();
            }

            throw new TimeoutException(errorMessage());
        }

        static TimeSpan NormalizeTimeout(TimeSpan timeout)
        {
            if (timeout == TimeSpan.MaxValue)
            {
                return Timeout.InfiniteTimeSpan;
            }

            if (timeout.TotalMilliseconds > int.MaxValue)
            {
                return TimeSpan.FromMilliseconds(int.MaxValue);
            }

            if (timeout < TimeSpan.Zero)
            {
                return TimeSpan.Zero;
            }

            return timeout;
        }

        static CancellationTokenSource CreateLinkedCancellationTokenSource(CancellationToken token)
        {
            return token.CanBeCanceled ? CancellationTokenSource.CreateLinkedTokenSource(token) : new CancellationTokenSource();
        }

        public static void IgnoreFault(this Task task)
        {
            if (task.IsCompleted)
            {
                var ignored = task.Exception;
            }
            else
            {
                task.ContinueWith(t => { var ignored = t.Exception; },
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
            }
        }

        public static void OnFault(this Task task, Action<Task> faultAction)
        {
            switch (task.Status)
            {
                case TaskStatus.RanToCompletion:
                case TaskStatus.Canceled:
                    break;
                case TaskStatus.Faulted:
                    faultAction(task);
                    break;
                default:
                    task.ContinueWith(faultAction, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
                    break;
            }
        }

        public static void OnFault(this Task task, Action<Task, object> faultAction, object state)
        {
            switch (task.Status)
            {
                case TaskStatus.RanToCompletion:
                case TaskStatus.Canceled:
                    break;
                case TaskStatus.Faulted:
                    faultAction(task, state);
                    break;
                default:
                    task.ContinueWith(faultAction, state, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
                    break;
            }
        }
    }
}