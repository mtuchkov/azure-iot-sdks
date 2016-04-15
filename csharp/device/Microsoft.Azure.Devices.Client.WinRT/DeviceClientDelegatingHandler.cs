// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
namespace Microsoft.Azure.Devices.Client
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    abstract class DeviceClientDelegatingHandler : IDelegatingHandler
    {
        protected IDelegatingHandler innerHandler;
        
        public IDelegatingHandler InnerHandler
        {
            get { return this.innerHandler; }
            set { this.innerHandler = value; }
        }

        public virtual Task AbandonAsync(string lockToken)
        {
            throw new NotImplementedException();
        }

        public virtual Task CloseAsync()
        {
            throw new NotImplementedException();
        }

        public virtual Task CompleteAsync(string lockToken)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            this.Dispose(true);
        }

        ~DeviceClientDelegatingHandler()
        {
            this.Dispose(false);
        }

        public virtual void Dispose(bool disposing)
        {
            throw new NotImplementedException();
        }

        public virtual Task OpenAsync(bool explicitOpen)
        {
            throw new NotImplementedException();
        }

        public virtual Task<Message> ReceiveAsync()
        {
            throw new NotImplementedException();
        }

        public virtual Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public virtual Task RejectAsync(string lockToken)
        {
            throw new NotImplementedException();
        }

        public virtual Task SendEventAsync(Message message)
        {
            throw new NotImplementedException();
        }

        public virtual Task SendEventAsync(IEnumerable<Message> messages)
        {
            throw new NotImplementedException();
        }
    }
}