// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Concurrent;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    class MqttIotHubAdapterFactory 
    {
        readonly TypeLoader typeLoader;

        public MqttIotHubAdapterFactory(TypeLoader typeLoader)
        {
            this.typeLoader = typeLoader;
        }

        public MqttIotHubAdapter Create(
            Action onConnected, 
            Action onDisconnected, 
            Action<Message> onMessageReceived, 
            IotHubConnectionString iotHubConnectionString, 
            Settings settings)
        {
            var persistanceProvider = this.typeLoader.LoadImplementation<ISessionStatePersistenceProvider>();
            var topicNameRouter = this.typeLoader.LoadImplementation<ITopicNameRouter>();
            IWillMessageProvider willMessageProvider = settings.HasWill ? this.typeLoader.LoadImplementation<IWillMessageProvider>() : null;
            
            return new MqttIotHubAdapter(
                iotHubConnectionString.DeviceId,
                iotHubConnectionString.HostName,
                iotHubConnectionString.GetPassword(),
                settings,
                persistanceProvider,
                topicNameRouter,
                willMessageProvider,
                onConnected,
                onDisconnected,
                onMessageReceived);
        }
    }
}