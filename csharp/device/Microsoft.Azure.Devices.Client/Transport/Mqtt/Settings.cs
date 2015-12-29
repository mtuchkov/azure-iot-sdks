// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using DotNetty.Codecs.Mqtt.Packets;

    class Settings
    {
        const bool DefaultCleanSession = true;
        const bool DefaultDeviceReceiveAckCanTimeout = false;
        const bool DefaultHasWill = false;
        const bool DefaultMaxOutboundRetransmissionEnforced = false;
        const int DefaultKeepAliveInSeconds = 300;
        const int DefaultMaxPendingOutboundMessages = 50;
        const int DefaultMaxPendingInboundMessages = 50;
        const int DefaultMaxOutboundRetransmissionCount = 0;
        static readonly TimeSpan DefaultDeviceReceiveAckTimeout = TimeSpan.FromSeconds(300);
        static readonly TimeSpan DefaultMaxKeepAliveTimeout = TimeSpan.FromSeconds(DefaultKeepAliveInSeconds / 2d);
        static readonly QualityOfService DefaultQualityOfService = QualityOfService.AtLeastOnce;

        public Settings(ISettingsProvider settingsProvider)
        {
            this.KeepAliveInSeconds = settingsProvider.GetIntegerSetting("KeepAliveInSeconds", DefaultKeepAliveInSeconds);
            this.CleanSession = settingsProvider.GetBooleanSetting("CleanSession", DefaultCleanSession);
            this.ConnectArrivalTimeout = settingsProvider.GetTimeSpanSetting("ConnectArrivalTimeout");
            this.HasWill = settingsProvider.GetBooleanSetting("HasWill", DefaultHasWill);
            this.MaxKeepAliveTimeout = settingsProvider.GetTimeSpanSetting("MaxKeepAliveTimeout", DefaultMaxKeepAliveTimeout);
            this.DefaultPublishToServerQoS = settingsProvider.GetEnumSetting("DefaultPublishToServerQoS", DefaultQualityOfService);
            this.DeviceReceiveAckCanTimeout = settingsProvider.GetBooleanSetting("DeviceReceiveAckCanTimeout", DefaultDeviceReceiveAckCanTimeout);
            this.DeviceReceiveAckTimeout = settingsProvider.GetTimeSpanSetting("DeviceReceiveAckTimeout", DefaultDeviceReceiveAckTimeout);
            this.MaxPendingOutboundMessages = settingsProvider.GetIntegerSetting("MaxPendingOutboundMessages", DefaultMaxPendingOutboundMessages);
            this.MaxPendingInboundMessages = settingsProvider.GetIntegerSetting("MaxPendingInboundMessages", DefaultMaxPendingInboundMessages);
            this.MaxOutboundRetransmissionCount = settingsProvider.GetULongSetting("MaxOutboundRetransmissionCount", DefaultMaxOutboundRetransmissionCount);
            this.MaxOutboundRetransmissionEnforced = settingsProvider.GetBooleanSetting("MaxOutboundRetransmissionEnforced", DefaultMaxOutboundRetransmissionEnforced);
        }

        public bool DeviceReceiveAckCanTimeout { get; set; }

        public TimeSpan? DeviceReceiveAckTimeout { get; set; }

        public TimeSpan? MaxKeepAliveTimeout { get; set; }

        public QualityOfService DefaultPublishToServerQoS { get; set; }

        public string RetainPropertyName { get; set; }

        public string DupPropertyName { get; set; }

        public string QoSPropertyName { get; set; }

        public int MaxPendingOutboundMessages { get; set; }

        public ulong MaxOutboundRetransmissionCount { get; set; }

        public bool MaxOutboundRetransmissionEnforced { get; set; }

        public int MaxPendingInboundMessages { get; set; }

        public TimeSpan? ConnectArrivalTimeout { get; set; }

        public bool CleanSession { get; set; }

        public int KeepAliveInSeconds { get; set; }

        public bool HasWill { get; set; }
    }
}