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
        class IotHubTransientErrorIgnoreStrategy:ITransientErrorDetectionStrategy
        {
            public bool IsTransient(Exception ex)
            {
                return ex is IotHubClientTransientException;
            }
        }

        internal int retrycount;

        readonly RetryPolicy retryPolicy;
        public RetryDelegatingHandler(DefaultDelegatingHandler innerHandler)
            :base(innerHandler)
        {
            this.retryPolicy = new RetryPolicy(new IotHubTransientErrorIgnoreStrategy(), 15, TimeSpan.FromMilliseconds(10), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));
            this.retryPolicy.Retrying += RetryPolicy_Retrying;
        }

        void RetryPolicy_Retrying(object sender, RetryingEventArgs e)
        {
            Interlocked.Increment(ref this.retrycount);
        }

        public override async Task SendEventAsync(Message message)
        {
            try
            {
                await this.retryPolicy.ExecuteAsync(() => base.SendEventAsync(message));
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

        public override async Task SendEventAsync(IEnumerable<Message> messages)
        {
            try
            {
                await this.retryPolicy.ExecuteAsync(() => base.SendEventAsync(messages));
            }
            catch (IotHubClientTransientException ex)
            {
                GetNormalizedIotHubException(ex).Throw();
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