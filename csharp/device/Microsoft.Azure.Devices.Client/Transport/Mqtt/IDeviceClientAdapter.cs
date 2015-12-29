// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System.Threading.Tasks;
    using DotNetty.Common.Concurrency;

    interface IDeviceClientAdapter
    {
        Task ConnectAsync { get; set; }
        
        Task DisconnectAsync { get; set; }

        Task SendEventAsync(Message message);

        Task<Message> ReceiveAsync();

        Task CompleteAsync(string lockToken);
    }
}