// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
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

    sealed class ErrorDelegatingHandler : DefaultDelegatingHandler
    {
        readonly Func<DefaultDelegatingHandler> handlerFactory;

        volatile TaskCompletionSource openCompletion;
        internal volatile Exception fatalException;
        internal int resetCounter;

        public ErrorDelegatingHandler(Func<DefaultDelegatingHandler> handlerFactory)
        {
            this.handlerFactory = handlerFactory;
        }

        public override async Task OpenAsync(bool explicitOpen)
        {
            if (this.fatalException != null)
            {
                throw this.fatalException;
            }
            TaskCompletionSource openPromise = this.openCompletion;
            if (openPromise == null)
            {
                openPromise = new TaskCompletionSource();
#pragma warning disable 420 //Reference to volitile variable will not be treated as volatile which is not quite true in this case.
                TaskCompletionSource currentOpenPromise;
                if ((currentOpenPromise = Interlocked.CompareExchange(ref this.openCompletion, openPromise, null)) == null)
#pragma warning restore 420
                {
                    this.InnerHandler = this.handlerFactory();
                    try
                    {
                        await this.ExecuteWithErrorHandlingAsync(() => base.OpenAsync(explicitOpen));
                        openPromise.TryComplete();
                    }
                    catch (Exception ex) when (!ex.IsFatal())
                    {
                        openPromise.TrySetException(ex);
                        throw;
                    }
                }
                else
                {
                    await currentOpenPromise.Task;
                }
            }
            else
            {
                await openPromise.Task;
            }
        }

        public override Task<Message> ReceiveAsync()
        {
            return this.ExecuteWithErrorHandlingAsync(() => base.ReceiveAsync());
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
            await this.EnsureOpenAsync();

            TaskCompletionSource completedPromise = this.openCompletion;

            IDelegatingHandler handler = this.InnerHandler;
            try
            {
                return await asyncOperation();
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                if (this.IsTransient(ex))
                {
                    if (this.IsTransportWorking(ex))
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
            await this.EnsureOpenAsync();

            TaskCompletionSource completedPromise = this.openCompletion;
            IDelegatingHandler handler = this.InnerHandler;

            try
            {
                await asyncOperation();
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                if (this.IsTransient(ex))
                {
                    if (this.IsTransportWorking(ex))
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

        Task EnsureOpenAsync()
        {
            return this.OpenAsync(false);
        }

        bool IsTransportWorking(Exception exception)
        {
            return exception.Unwind<IotHubClientTransientException>().Any();
        }

        bool IsTransient(Exception exception)
        {
            IEnumerable<IotHubException> iotHubExceptions = exception.Unwind<IotHubException>();
            bool hasUnauthorized = iotHubExceptions.Any(x => x.Unwind<UnauthorizedException>().Any());

            return !hasUnauthorized && (iotHubExceptions.Any() || exception.Unwind<IOException>().Any() || exception.Unwind<TimeoutException>().Any() || exception.Unwind<ObjectDisposedException>().Any() || exception.Unwind<OperationCanceledException>().Any()
#if !PCL && !WINDOWS_UWP
                || exception.Unwind<System.Net.Sockets.SocketException>().Any()
#endif
                );
        }

        void Reset(TaskCompletionSource completion, IDelegatingHandler handler)
        {
            if (completion == this.openCompletion)
            {
#pragma warning disable 420 //Reference to volitile variable will not be treated as volatile which is not quite true in this case.
                if (Interlocked.CompareExchange(ref this.openCompletion, null, completion) == completion)
#pragma warning restore 420
                {
                    if (handler == this.InnerHandler)
                    {
                        this.Cleanup(handler);
                    }
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
                }
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                //unexpected behaviour - ignore. LOG?
            }
        }
    }
}