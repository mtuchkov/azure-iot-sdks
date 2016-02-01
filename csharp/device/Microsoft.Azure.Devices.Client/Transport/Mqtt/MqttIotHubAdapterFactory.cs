// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    class MqttIotHubAdapterFactory 
    {
        readonly MqttTransportSettings settings;

        public MqttIotHubAdapterFactory(MqttTransportSettings settings)
        {
            this.settings = settings;
        }

        public MqttIotHubAdapter Create(
            Action onConnected, 
            Action<Message> onMessageReceived,
            Action<Exception> onError, 
            IotHubConnectionString iotHubConnectionString, 
            MqttTransportSettings mqttTransportSettings)
        {
            ISessionStatePersistenceProvider persistenceProvider = this.settings.SessionStatePersistenceProvider;
            IWillMessageProvider willMessageProvider = mqttTransportSettings.HasWill ? this.settings.WillMessageProvider : null;
            
            return new MqttIotHubAdapter(
                iotHubConnectionString.DeviceId,
                iotHubConnectionString.HostName,
                iotHubConnectionString.GetPassword(),
                mqttTransportSettings,
                persistenceProvider,
                willMessageProvider,
                onConnected,
                onMessageReceived,
                onError);
        }
    }
}