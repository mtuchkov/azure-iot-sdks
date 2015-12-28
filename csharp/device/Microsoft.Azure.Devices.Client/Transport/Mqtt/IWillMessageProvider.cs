// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System.Collections.Generic;
    using DotNetty.Codecs.Mqtt.Packets;

    interface IWillMessageProvider
    {
        string Message { get; }

        QualityOfService QoS { get; set; }

        IDictionary<string, string> Properties { get; set; }
    }
}