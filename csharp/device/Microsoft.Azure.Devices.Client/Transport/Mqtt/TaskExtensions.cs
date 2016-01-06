// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Extensions;

    static class TaskExtensions
    {
        public static async Task<T> WithTimeoutAsync<T>(this Func<Task<T>> asyncOperation, TimeSpan timeout)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            Task<T> timedOperationTask = TimedOperationAsync(cancellationTokenSource, asyncOperation);
            await Task.WhenAny(timedOperationTask, timedOperationTask);
            cancellationTokenSource.Cancel();
            return timedOperationTask.Result;
        }

        static async Task<T> TimedOperationAsync<T>(CancellationTokenSource cancellationTokenSource, Func<Task<T>> asyncOperation)
        {
            try
            {
                return await asyncOperation();
            }
            catch (OperationCanceledException ex)
            {
                if (ex.CancellationToken == cancellationTokenSource.Token)
                {
                    return default(T);
                }
                throw;
            }
            catch (Exception ex)
            {
                if (ex.IsFatal())
                {
                    throw;
                }
                cancellationTokenSource.Cancel();
                throw;
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