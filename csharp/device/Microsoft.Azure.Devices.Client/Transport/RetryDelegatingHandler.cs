// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

    class RetryDelegatingHandler : DefaultDelegatingHandler
    {
        class SendMessageState
        {
            public int Iteration { get; set; }

            public long InitialStreamPosition { get; set; }

            public ExceptionDispatchInfo OriginalError { get; set; }
        }

        class IotHubTransientErrorIgnoreStrategy:ITransientErrorDetectionStrategy
        {
            public bool IsTransient(Exception ex)
            {
                return ex is IotHubClientTransientException && (ex.Data["stopRetrying"] == null || !(bool)ex.Data["stopRetrying"]);
            }
        }

        //Only for debug
        internal int retrycount;

        readonly RetryPolicy retryPolicy;
        public RetryDelegatingHandler(IDelegatingHandler innerHandler)
            :base(innerHandler)
        {
            this.retryPolicy = new RetryPolicy(new IotHubTransientErrorIgnoreStrategy(), 15, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
            //Only for debug
            this.retryPolicy.Retrying += RetryPolicy_Retrying;
        }

        //Only for debug
        void RetryPolicy_Retrying(object sender, RetryingEventArgs e)
        {
            Interlocked.Increment(ref this.retrycount);
        }

        public override async Task SendEventAsync(Message message)
        {
            try
            {
                var sendState = new SendMessageState();
                await this.retryPolicy.ExecuteAsync(() => this.SendMessageWithRetryAsync(sendState, message, () => base.SendEventAsync(message)));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
            }
        }

        public override async Task SendEventAsync(IEnumerable<Message> messages)
        {
            try
            {
                var sendState = new SendMessageState();
                await this.retryPolicy.ExecuteAsync(() => this.SendMessageWithRetryAsync(sendState, messages, () => base.SendEventAsync(messages)));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
            }
        }

        public override async Task<Message> ReceiveAsync()
        {
            try
            {
                return await this.retryPolicy.ExecuteAsync(() => base.ReceiveAsync());
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
                throw;
            }
        }

        public override async Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            try
            {
                return await this.retryPolicy.ExecuteAsync(() => base.ReceiveAsync(timeout));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
                throw;
            }
        }

        public override async Task CompleteAsync(string lockToken)
        {
            try
            {
                await this.retryPolicy.ExecuteAsync(() => base.CompleteAsync(lockToken));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
            }
        }

        public override async Task AbandonAsync(string lockToken)
        {
            try
            {
                await this.retryPolicy.ExecuteAsync(() => base.AbandonAsync(lockToken));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
            }
        }

        public override async Task RejectAsync(string lockToken)
        {
            try
            {
                await this.retryPolicy.ExecuteAsync(() => base.RejectAsync(lockToken));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
            }
        }

        public override async Task OpenAsync(bool explicitOpen)
        {
            try
            {
                await this.retryPolicy.ExecuteAsync(() => base.OpenAsync(explicitOpen));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
            }
        }

        async Task SendMessageWithRetryAsync(SendMessageState sendState, IEnumerable<Message> messages, Func<Task> action)
        {
            if (sendState.Iteration == 0)
            {
                foreach (Message message in messages)
                {
                    sendState.InitialStreamPosition = message.BodyStream.Position > 0 ? message.BodyStream.Position : 0;
                    message.ResetGetBodyCalled();
                }

                await TryExecuteActionAsync(sendState, action);
                return;
            }

            EnsureStreamsAreInOriginalState(sendState, messages);

            await TryExecuteActionAsync(sendState, action);
        }

        async Task SendMessageWithRetryAsync(SendMessageState sendState, Message message, Func<Task> action)
        {
            if (sendState.Iteration == 0)
            {
                sendState.InitialStreamPosition = message.BodyStream.Position;
                message.ResetGetBodyCalled();

                await TryExecuteActionAsync(sendState, action);
                return;
            }

            EnsureStreamIsInOriginalState(sendState, message);

            await TryExecuteActionAsync(sendState, action);
        }

        static void EnsureStreamsAreInOriginalState(SendMessageState sendState, IEnumerable<Message> messages)
        {
            foreach (Message message in messages)
            {
                if (!message.BodyStream.CanRead)
                {
                    sendState.OriginalError.SourceException.Data["stopRetrying"] = true;
                    sendState.OriginalError.Throw();
                }

                if (message.BodyStream.Position == 0)
                {
                    message.ResetGetBodyCalled();
                }

                //Initial stream position can be not 0 intentionally, i.e. users may want to send the message starting from offset.
                //However, we don't want to store initial positions of each message, so we just leave this rare scenario to the user.
                else if (message.BodyStream.CanSeek && sendState.InitialStreamPosition == 0)
                {
                    message.BodyStream.Position = 0;
                    message.ResetGetBodyCalled();
                }
                else
                {
                    sendState.OriginalError.SourceException.Data["stopRetrying"] = true;
                    sendState.OriginalError.Throw();
                }
            }
        }

        static void EnsureStreamIsInOriginalState(SendMessageState sendState, Message message)
        {
            if (!message.BodyStream.CanRead)
            {
                sendState.OriginalError.SourceException.Data["stopRetrying"] = true;
                sendState.OriginalError.Throw();
            }

            if (message.BodyStream.Position == sendState.InitialStreamPosition)
            {
                message.ResetGetBodyCalled();
            }
            else if (message.BodyStream.CanSeek)
            {
                message.BodyStream.Position = sendState.InitialStreamPosition;
                message.ResetGetBodyCalled();
            }
            else
            {
                sendState.OriginalError.SourceException.Data["stopRetrying"] = true;
                sendState.OriginalError.Throw();
            }
        }

        static async Task TryExecuteActionAsync(SendMessageState sendState, Func<Task> action)
        {
            sendState.Iteration++;
            try
            {
                await action();
            }
            catch (IotHubClientTransientException ex)
            {
                sendState.OriginalError = ExceptionDispatchInfo.Capture(ex);
                throw;
            }
        }

        static ExceptionDispatchInfo GetNormalizedIotHubException(IotHubClientTransientException ex)
        {
            if (ex.InnerException != null)
            {
                return ExceptionDispatchInfo.Capture(ex.InnerException);
            }
            return ExceptionDispatchInfo.Capture(ex);
        }
    }
}