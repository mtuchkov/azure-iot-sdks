// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System.Collections.Generic;
    using DotNetty.Codecs.Mqtt.Packets;

    public class StaticWillMessageProvider : IWillMessageProvider
    {
        public Message Message { get; private set; }

        public QualityOfService QoS { get; set; }

        public IDictionary<string, string> Properties { get; set; }

        public StaticWillMessageProvider(QualityOfService qos, Message message, IDictionary<string, string> properties)
        {
            this.QoS = qos;
            this.Message = message;
            this.Properties = properties;
        }
    }
}