// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    interface IMqttIotHubAdapter
    {
        Task SendEventAsync(Message repeat);

        Task SendEventAsync(Message message, IDictionary<string, string> properties);

        Task<Message> ReceiveAsync(TimeSpan timeout);
    }
}