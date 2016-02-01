// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Xml;
    using DotNetty.Codecs.Mqtt.Packets;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    class MqttTransportSettings
    {
        const bool DefaultCleanSession = false;
        const bool DefaultDeviceReceiveAckCanTimeout = false;
        const bool DefaultHasWill = false;
        const bool DefaultMaxOutboundRetransmissionEnforced = false;
        const int DefaultKeepAliveInSeconds = 300;
        const int DefaultMaxPendingOutboundMessages = 50;
        const int DefaultMaxPendingInboundMessages = 50;
        const int DefaultMaxOutboundRetransmissionCount = 0;
        const QualityOfService DefaultPublishToServerQoS = QualityOfService.AtLeastOnce;
        const QualityOfService DefaultReceivingQoS = QualityOfService.AtLeastOnce;
        static readonly TimeSpan DefaultConnectArrivalTimeout = TimeSpan.FromSeconds(300);
        static readonly TimeSpan DefaultDeviceReceiveAckTimeout = TimeSpan.FromSeconds(300);
        static readonly TimeSpan DefaultMaxKeepAliveTimeout = TimeSpan.FromSeconds(DefaultKeepAliveInSeconds / 2d);

        public MqttTransportSettings()
        {
            this.CleanSession = DefaultCleanSession;
            this.ConnectArrivalTimeout = DefaultConnectArrivalTimeout;
            this.DeviceReceiveAckCanTimeout = DefaultDeviceReceiveAckCanTimeout;
            this.DeviceReceiveAckTimeout = DefaultDeviceReceiveAckTimeout;
            this.DupPropertyName = "mqtt-dup";
            this.HasWill = DefaultHasWill;
            this.KeepAliveInSeconds = DefaultKeepAliveInSeconds;
            this.MaxOutboundRetransmissionEnforced = DefaultMaxOutboundRetransmissionEnforced;
            this.MaxPendingInboundMessages = DefaultMaxPendingInboundMessages;
            this.PublishToServerQoS = DefaultPublishToServerQoS;
            this.ReceivingQoS = DefaultReceivingQoS;
            this.QoSPropertyName = "mqtt-qos";
            this.RetainPropertyName = "mqtt-retain";
            this.SessionStatePersistenceProvider = new InMemorySessionStateProvider();
            this.WillMessageProvider = null;
        }

        public MqttTransportSettings(XmlElement parent)
        {
            this.CleanSession = this.GetBoolean(parent, "cleanSession", DefaultCleanSession);
            this.ConnectArrivalTimeout = this.GetTimeSpan(parent, "connectArrivalTimeout", DefaultConnectArrivalTimeout);
            this.DeviceReceiveAckTimeout = this.GetTimeSpan(parent, "deviceReceiveAckCanTimeout", DefaultDeviceReceiveAckTimeout);
            this.DeviceReceiveAckCanTimeout = this.GetBoolean(parent, "deviceReceiveAckCanTimeout", DefaultDeviceReceiveAckCanTimeout);
            this.DupPropertyName = parent.GetAttribute("dupPropertyName");
            this.HasWill = this.GetBoolean(parent, "hasWill", DefaultHasWill);
            this.KeepAliveInSeconds = this.GetInteger(parent, "keepAliveInSeconds", DefaultKeepAliveInSeconds);
            this.MaxOutboundRetransmissionEnforced = this.GetBoolean(parent, "defaultMaxOutboundRetransmissionEnforced", DefaultMaxOutboundRetransmissionEnforced);
            this.MaxPendingInboundMessages = this.GetInteger(parent, "maxPendingInboundMessages", DefaultMaxPendingInboundMessages);
            this.PublishToServerQoS = this.GetEnum(parent, "publishToServerQoS", DefaultPublishToServerQoS);
            this.ReceivingQoS = this.GetEnum(parent, "receivingQoS", DefaultReceivingQoS);
            this.QoSPropertyName = parent.GetAttribute("qoSPropertyName");
            this.RetainPropertyName = "mqtt-retain";
            this.SessionStatePersistenceProvider = new InMemorySessionStateProvider(); // this.CreateImplementation<ISessionStatePersistenceProvider>(configuration.SessionStatePersistenceProviderTypeName, typeof(InMemorySessionStateProvider));
            this.WillMessageProvider = null;// this.CreateImplementation<IWillMessageProvider>(configuration.WillMessageProviderTypeName, typeof(WillMessageProvider));
        }

        public bool DeviceReceiveAckCanTimeout { get; set; }

        public TimeSpan DeviceReceiveAckTimeout { get; set; }

        public QualityOfService PublishToServerQoS { get; set; }

        public QualityOfService ReceivingQoS { get; set; }

        public string RetainPropertyName { get; set; }

        public string DupPropertyName { get; set; }

        public string QoSPropertyName { get; set; }

        public bool MaxOutboundRetransmissionEnforced { get; set; }

        public int MaxPendingInboundMessages { get; set; }

        public TimeSpan ConnectArrivalTimeout { get; set; }

        public bool CleanSession { get; set; }

        public int KeepAliveInSeconds { get; set; }

        public bool HasWill { get; set; }

        public ISessionStatePersistenceProvider SessionStatePersistenceProvider { get; set; }

        public IWillMessageProvider WillMessageProvider { get; set; }

        bool GetBoolean(XmlElement element, string settingName, bool defaultValue)
        {
            string data = element.GetAttribute(settingName);
            if (string.IsNullOrEmpty(data))
            {
                return bool.Parse(data);
            }
            return defaultValue;
        }

        TimeSpan GetTimeSpan(XmlElement element, string settingName, TimeSpan defaultValue)
        {
            string data = element.GetAttribute(settingName);
            if (string.IsNullOrEmpty(data))
            {
                return TimeSpan.Parse(data);
            }
            return defaultValue;
        }

        int GetInteger(XmlElement element, string settingName, int defaultValue)
        {
            string data = element.GetAttribute(settingName);
            if (string.IsNullOrEmpty(data))
            {
                return int.Parse(data);
            }
            return defaultValue;
        }

        ulong GetULong(XmlElement element, string settingName, ulong defaultValue)
        {
            string data = element.GetAttribute(settingName);
            if (string.IsNullOrEmpty(data))
            {
                return ulong.Parse(data);
            }
            return defaultValue;
        }

        T GetEnum<T>(XmlElement element, string settingName, T defaultValue) where T : struct
        {
            string data = element.GetAttribute(settingName);
            if (string.IsNullOrEmpty(data))
            {
                return (T)Enum.Parse(typeof(T), data);
            }
            return defaultValue;
        }

        T CreateImplementation<T>(string implTypeName, Type defaultImplType) where T : class
        {
            Type type = Type.GetType(implTypeName);
            var impl = Activator.CreateInstance(AppDomain.CurrentDomain, type.AssemblyQualifiedName, type.FullName) as T;
            if (impl != null)
            {
                impl = Activator.CreateInstance(AppDomain.CurrentDomain, defaultImplType.AssemblyQualifiedName, defaultImplType.FullName) as T;
            }
            return impl;
        }
    }
}