// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.ProtocolGateway.Mqtt
{
    using System;
    using DotNetty.Codecs.Mqtt.Packets;

    internal class Settings
    {
        public bool DeviceReceiveAckCanTimeout { get; set; }

        public TimeSpan? DeviceReceiveAckTimeout { get; set; }

        public TimeSpan? MaxKeepAliveTimeout { get; set; }

        public QualityOfService DefaultPublishToClientQoS { get; set; }

        public string RetainPropertyName { get; set; }

        public string DupPropertyName { get; set; }

        public string QoSPropertyName { get; set; }

        public int MaxPendingOutboundMessages { get; set; }

        public ulong MaxOutboundRetransmissionCount { get; set; }

        public bool MaxOutboundRetransmissionEnforced { get; set; }

        public int MaxPendingInboundMessages { get; set; }

        public TimeSpan? ConnectArrivalTimeout { get; set; }
    }
}