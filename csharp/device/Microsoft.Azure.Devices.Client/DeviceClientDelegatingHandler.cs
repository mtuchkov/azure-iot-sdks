// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Common;

    abstract class DeviceClientDelegatingHandler : IDelegatingHandler
    {
        static readonly Task<Message> DummyResultObject = Task.FromResult((Message)null);

        protected volatile IDelegatingHandler innerHandler;

        public IDelegatingHandler InnerHandler
        {
            get { return this.innerHandler; }
            set { this.innerHandler = value; }
        }

        protected DeviceClientDelegatingHandler()
            : this(null)
        {
        }

        protected DeviceClientDelegatingHandler(DeviceClientDelegatingHandler innerHandler)
        {
            this.InnerHandler = innerHandler;
        }

        public virtual Task OpenAsync(bool explicitOpen)
        {
            return this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler.OpenAsync(explicitOpen);
        }

        public virtual async Task CloseAsync()
        {
            await (this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler.CloseAsync()).ContinueWith(t => GC.SuppressFinalize(this), TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        public virtual Task<Message> ReceiveAsync()
        {
            return this.InnerHandler == null ? DummyResultObject : this.InnerHandler.ReceiveAsync();
        }

        public virtual Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            return this.InnerHandler == null ? DummyResultObject : this.InnerHandler.ReceiveAsync(timeout);
        }

        public virtual Task CompleteAsync(string lockToken)
        {
            return this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler.CompleteAsync(lockToken);
        }

        public virtual Task AbandonAsync(string lockToken)
        {
            return this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler.AbandonAsync(lockToken);
        }

        public virtual Task RejectAsync(string lockToken)
        {
            return this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler.RejectAsync(lockToken);
        }

        public virtual Task SendEventAsync(Message message)
        {
            return this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler.SendEventAsync(message);
        }

        public virtual Task SendEventAsync(IEnumerable<Message> messages)
        {
            return this.InnerHandler == null ? TaskConstants.Completed : this.InnerHandler.SendEventAsync(messages);
        }

        public virtual void Dispose(bool disposing)
        {
            
        }

        public void Dispose()
        {
            this.Dispose(true);   
            GC.SuppressFinalize(this);
        }

        ~DeviceClientDelegatingHandler()
        {
            this.Dispose(false);
        }
    }
}