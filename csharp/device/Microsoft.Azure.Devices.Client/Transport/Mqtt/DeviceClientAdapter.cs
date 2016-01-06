// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System.Threading.Tasks;
    using DotNetty.Common.Concurrency;

    class DeviceClientAdapter
    {
        readonly TaskCompletionSource connectTaskCompletionSource;
        readonly TaskCompletionSource disconnectTaskCompletionSource;

        public DeviceClientAdapter()
        {
            this.connectTaskCompletionSource = new TaskCompletionSource();
            this.ConnectAsync = this.connectTaskCompletionSource.Task;
            this.DisconnectAsync = this.connectTaskCompletionSource.Task;
           
        }
        public Task ConnectAsync { get; set; }

        public Task DisconnectAsync { get; set; }

        void OnDisconnected()
        {
            this.disconnectTaskCompletionSource.Complete();
        }

        void OnCnnected()
        {
            this.connectTaskCompletionSource.Complete();
        }

        public Task SendEventAsync(Message message)
        {
            throw new System.NotImplementedException();
        }

        public Task<Message> ReceiveAsync()
        {
            throw new System.NotImplementedException();
        }

        public Task CompleteAsync(string lockToken)
        {
            throw new System.NotImplementedException();
        }
    }
}