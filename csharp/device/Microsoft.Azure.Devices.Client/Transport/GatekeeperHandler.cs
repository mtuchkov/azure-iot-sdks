// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport
{

    // C# using aliases cannot name an unbound generic type declaration without supplying type arguments
    // Therefore, define a separate alias for each type argument
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Contains the implementation of methods that a device can use to send messages to and receive from the service.
    /// </summary>
    sealed class GatekeeperHandler : DeviceClientDelegatingHandler
    {
        internal bool OpenCalled;
        internal bool CloseCalled;
        volatile TaskCompletionSource<object> openTaskCompletionSource;

        public GatekeeperHandler()
        {
            this.ThisLock = new object();
            this.openTaskCompletionSource = new TaskCompletionSource<object>(this);
        }
        
        protected object ThisLock { get; }

        /// <summary>
        /// Explicitly open the DeviceClient instance.
        /// </summary>
        public Task OpenAsync()
        {
            return this.EnsureOpenedAsync(true);
        }

        /// <summary>
        /// Close the DeviceClient instance
        /// </summary>
        /// <returns></returns>
        public override Task CloseAsync()
        {
            TaskCompletionSource<object> localOpenTaskCompletionSource;
            lock (this.ThisLock)
            {
                if (this.CloseCalled)
                {
                    return TaskHelpers.CompletedTask;
                }

                localOpenTaskCompletionSource = this.openTaskCompletionSource;
                this.CloseCalled = true;
            }

            localOpenTaskCompletionSource?.TrySetCanceled();

            return base.CloseAsync();
        }

        /// <summary>
        /// Receive a message from the device queue with the specified timeout
        /// </summary>
        /// <returns>The receive message or null if there was no message until the specified time has elapsed</returns>
        public override async Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            TimeoutHelper.ThrowIfNegativeArgument(timeout);
            await this.EnsureOpenedAsync(false);
            return await base.ReceiveAsync(timeout);
        }

        /// <summary>
        /// Deletes a received message from the device queue
        /// </summary>
        /// <returns>The lock identifier for the previously received message</returns>
        public override async Task CompleteAsync(string lockToken)
        {
            await this.EnsureOpenedAsync(false);
            await base.CompleteAsync(lockToken);
        }

        /// <summary>
        /// Puts a received message back onto the device queue
        /// </summary>
        /// <returns>The previously received message</returns>
        public override async Task AbandonAsync(string lockToken)
        {
            await this.EnsureOpenedAsync(false);
            await base.AbandonAsync(lockToken);
        }

        /// <summary>
        /// Deletes a received message from the device queue and indicates to the server that the message could not be processed.
        /// </summary>
        /// <returns>The previously received message</returns>
        public override async Task RejectAsync(string lockToken)
        {
            await this.EnsureOpenedAsync(false);
            await base.RejectAsync(lockToken);
        }

        /// <summary>
        /// Sends an event to device hub
        /// </summary>
        /// <returns>The message containing the event</returns>
        public override async Task SendEventAsync(Message message)
        {
            await this.EnsureOpenedAsync(false);
            await base.SendEventAsync(message);
        }

        /// <summary>
        /// Sends a batch of events to device hub
        /// </summary>
        /// <returns>The task containing the event</returns>
        public override async Task SendEventAsync(IEnumerable<Message> messages)
        {
            await this.EnsureOpenedAsync(false);
            await base.SendEventAsync(messages);
        }

        protected Task EnsureOpenedAsync(bool explicitOpen)
        {
            bool needOpen = false;
            Task openTask;
            if (this.openTaskCompletionSource != null)
            {
                lock (this.ThisLock)
                {
                    if (this.OpenCalled)
                    {
                        if (this.openTaskCompletionSource == null)
                        {
                            // openTaskCompletionSource being null means open has finished completely
                            openTask = TaskHelpers.CompletedTask;
                        }
                        else
                        {
                            openTask = this.openTaskCompletionSource.Task;
                        }
                    }
                    else
                    {
                        // It's this call's job to kick off the open.
                        this.OpenCalled = true;
                        openTask = this.openTaskCompletionSource.Task;
                        needOpen = true;
                    }
                }
            }
            else
            {
                // Open has already fully completed.
                openTask = TaskHelpers.CompletedTask;
            }

            if (needOpen)
            {
                base.OpenAsync(explicitOpen).ContinueWith(
                    t =>
                    {
                        TaskCompletionSource<object> localOpenTaskCompletionSource = this.openTaskCompletionSource;
                        lock (this.ThisLock)
                        {
                            if (!t.IsFaulted && !t.IsCanceled)
                            {
                                // This lets future calls avoid the Open logic all together.
                                this.openTaskCompletionSource = null;
                            }
                            else
                            {
                                // OpenAsync was cancelled or threw an exception, next time retry.
                                this.OpenCalled = false;
                                this.openTaskCompletionSource = new TaskCompletionSource<object>(this);
                            }
                        }

                        // This completes anyone waiting for open to finish
                        TaskHelpers.MarshalTaskResults(t, localOpenTaskCompletionSource);
                    });
            }

            return openTask;
        }
    }
}
