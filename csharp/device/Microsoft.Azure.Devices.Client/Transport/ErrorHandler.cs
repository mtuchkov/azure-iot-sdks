// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
#if !PCL && !WINDOWS_UWP
    using DotNetty.Common.Concurrency;
#endif
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;

    // Copyright (c) Microsoft. All rights reserved.
    // Licensed under the MIT license. See LICENSE file in the project root for full license information.

    sealed class ErrorHandler : DeviceClientDelegatingHandler
    {
        readonly Func<DeviceClientDelegatingHandler> handlerFactory;

        volatile TaskCompletionSource openCompletion;
        volatile Exception fatalException;

        public ErrorHandler(Func<DeviceClientDelegatingHandler> handlerFactory)
        {
            this.handlerFactory = handlerFactory;
        }

        public override async Task OpenAsync(bool explicitOpen)
        {
            if (this.fatalException != null)
            {
                throw this.fatalException;
            }
            await base.OpenAsync(explicitOpen);
            this.openCompletion.TryComplete();
        }

        public override Task<Message> ReceiveAsync()
        {
            return this.ExecuteWithErrorHandlingAsync(()=> base.ReceiveAsync());
        }

        public override Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            return this.ExecuteWithErrorHandlingAsync(() => base.ReceiveAsync(timeout));
        }

        public override Task AbandonAsync(string lockToken)
        {
            return this.ExecuteWithErrorHandlingAsync(() => base.AbandonAsync(lockToken));
        }

        public override Task CompleteAsync(string lockToken)
        {
            return this.ExecuteWithErrorHandlingAsync(() => base.CompleteAsync(lockToken));
        }

        public override Task RejectAsync(string lockToken)
        {
            return this.ExecuteWithErrorHandlingAsync(() => base.RejectAsync(lockToken));
        }

        public override Task SendEventAsync(IEnumerable<Message> messages)
        {
            return this.ExecuteWithErrorHandlingAsync(() => base.SendEventAsync(messages));
        }

        public override Task SendEventAsync(Message message)
        {
            return this.ExecuteWithErrorHandlingAsync(() => base.SendEventAsync(message));
        }

        async Task<T> ExecuteWithErrorHandlingAsync<T>(Func<Task<T>> asyncOperation)
        {
            TaskCompletionSource completedPromise = await this.EnsureReadyStateAsync();

            IDelegatingHandler handler = this.InnerHandler;
            try
            {
                return await asyncOperation();
            }
            catch(Exception ex) when (!ex.IsFatal())
            {
                if (this.IsTransient(ex))
                {
                    if (this.IsTransportWorking((IotHubException)ex))
                    {
                        throw;
                    }
                    this.Reset(completedPromise, handler);
                    throw;
                }
#pragma warning disable 420 //Reference to volitile variable will not be treated as volatile which is not quite true in this case.
                if (Interlocked.CompareExchange(ref this.fatalException, ex, null) == null)
#pragma warning restore 420
                {
                    await this.CloseAsync();
                }
                throw;
            }
        }

        async Task ExecuteWithErrorHandlingAsync(Func<Task> asyncOperation)
        {
            if (this.fatalException != null)
            {
                throw this.fatalException;
            }
            TaskCompletionSource completedPromise = await this.EnsureReadyStateAsync();
            IDelegatingHandler handler = this.InnerHandler;

            try
            {
                await asyncOperation();
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                if (this.IsTransient(ex))
                {
                    if (this.IsTransportWorking((IotHubException)ex))
                    {
                        throw;
                    }
                    this.Reset(completedPromise, handler);
                    throw;
                }

#pragma warning disable 420 //Reference to volitile variable will not be treated as volatile which is not quite true in this case.
                if (Interlocked.CompareExchange(ref this.fatalException, ex, null) == null)
#pragma warning restore 420
                {
                    await this.CloseAsync();
                }
                throw;
            }
        }

        bool IsTransportWorking(IotHubException exception)
        {
            return exception.Unwind<InvalidOperationException>().Any();
        }

        bool IsTransient(Exception exception)
        {
            IotHubException ex = exception.Unwind<IotHubException>().FirstOrDefault();
            return ex != null && !(ex is UnauthorizedException);
        }

        async Task<TaskCompletionSource> EnsureReadyStateAsync()
        {
#pragma warning disable 420 //Reference to volitile variable will not be treated as volatile which is not quite true in this case.
            if (this.openCompletion == null && Interlocked.CompareExchange(ref this.openCompletion, new TaskCompletionSource(), null) == null)
#pragma warning restore 420
            {
                await this.OpenAsync(false);
            }
            TaskCompletionSource readyPromise = this.openCompletion;
            await readyPromise.Task;
            return readyPromise;
        }

        void Reset(TaskCompletionSource completion, IDelegatingHandler handler)
        {
            if (completion == this.openCompletion)
            {
#pragma warning disable 420 //Reference to volitile variable will not be treated as volatile which is not quite true in this case.
                if (Interlocked.CompareExchange(ref this.openCompletion, new TaskCompletionSource(), completion) == completion)
#pragma warning restore 420
                {
                    this.Cleanup(handler);
                }
            }
        }

        async void Cleanup(IDelegatingHandler handler)
        {
            try
            {
                if (handler != null)
                {
                    await handler.CloseAsync();
                    if (handler == this.innerHandler)
                    {
#pragma warning disable 420 //Reference to volitile variable will not be treated as volatile which is not quite true in this case.
                        Interlocked.CompareExchange(ref this.innerHandler, this.handlerFactory(), handler);
#pragma warning restore 420
                    }
                }
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                //unexpected behaviour - ignore. LOG?
            }
        }
    }
}