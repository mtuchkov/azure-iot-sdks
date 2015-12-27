// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using DotNetty.Codecs.Mqtt;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Bootstrapping;
    using DotNetty.Transport.Channels;
    using DotNetty.Transport.Channels.Sockets;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Routing;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    sealed class MqttTransportHandler : TansportHandlerBase
    {
        const int ProtocolGatewayPort = 8883;

        readonly Bootstrap bootstrap;
        readonly IPAddress serverAddress;
        Func<Task> cleanupFunc;
        readonly IMqttIotHubAdapter mqttIotHubAdapter;

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString)
        {
            string targetHost = iotHubConnectionString.HostName;
            
            IPHostEntry hostEntry = Dns.GetHostEntry(targetHost);

            this.serverAddress = hostEntry != null && hostEntry.AddressList.Length > 0 ? 
                hostEntry.AddressList[0] : IPAddress.Loopback;
                
            var group = new SingleInstanceEventLoopGroup();

            Settings settings = this.LoadSettings();

            ISessionStatePersistenceProvider persistanceProvider = this.LoadPersistenceProvider(settings);

            ITopicNameRouter topicNameRouter = new TopicNameRouter();

            string clientId = iotHubConnectionString.HostName + "/" + iotHubConnectionString.DeviceId;
            var channelHandlerAdapter = new MqttIotHubAdapter(clientId, settings, persistanceProvider, topicNameRouter);

            this.mqttIotHubAdapter = channelHandlerAdapter;
            
            this.bootstrap = new Bootstrap()
                .Group(@group)
                .Channel<TcpSocketChannel>()
                .Option(ChannelOption.TcpNodelay, true)
                .Handler(new ActionChannelInitializer<ISocketChannel>(ch =>
                {
                    ch.Pipeline.AddLast(
                        TlsHandler.Client(targetHost, null),
                        MqttEncoder.Instance,
                        new MqttDecoder(false, 256 * 1024),
                        channelHandlerAdapter);
                }));

            this.ScheduleCleanup(async () =>
            {
                await group.ShutdownGracefullyAsync();
            });

        }

        /// <summary>
        /// Create a DeviceClient from individual parameters
        /// </summary>
        /// <param name="hostname">The fully-qualified DNS hostname of IoT Hub</param>
        /// <param name="authMethod">The authentication method that is used</param>
        /// <returns>DeviceClient</returns>
        public static MqttTransportHandler Create(string hostname, IAuthenticationMethod authMethod)
        {
            if (hostname == null)
            {
                throw new ArgumentNullException("hostname");
            }

            if (authMethod == null)
            {
                throw new ArgumentNullException("authMethod");
            }

            IotHubConnectionStringBuilder connectionStringBuilder = IotHubConnectionStringBuilder.Create(hostname, authMethod);
            return CreateFromConnectionString(connectionStringBuilder.ToString());
        }

        /// <summary>
        /// Create DeviceClient from the specified connection string
        /// </summary>
        /// <param name="connectionString">Connection string for the IoT hub</param>
        /// <returns>DeviceClient</returns>
        public static MqttTransportHandler CreateFromConnectionString(string connectionString)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException("connectionString");
            }

            IotHubConnectionString iotHubConnectionString = IotHubConnectionString.Parse(connectionString);
            
            return new MqttTransportHandler(iotHubConnectionString);
        }

        protected override TimeSpan DefaultReceiveTimeout { get; set; }

        protected override async Task OnOpenAsync(bool explicitOpen)
        {
            IChannel clientChannel = await this.bootstrap.ConnectAsync(this.serverAddress, ProtocolGatewayPort);
            
            this.ScheduleCleanup(async () =>
            {
                await clientChannel.CloseAsync();
            });
        }

        protected override Task OnCloseAsync()
        {
            return this.cleanupFunc();
        }

        protected override Task OnSendEventAsync(Message message)
        {
            return this.mqttIotHubAdapter.SendEventAsync(message);
        }

        protected override async Task OnSendEventAsync(IEnumerable<Message> messages)
        {
            foreach (Message message in messages)
            {
                await this.mqttIotHubAdapter.SendEventAsync(message);
            }
        }

        internal async Task SendEventAsync(IEnumerable<string> messages)
        {
            foreach (string messageString in messages)
            {
                var message = new Message(Encoding.UTF8.GetBytes(messageString));
                await this.mqttIotHubAdapter.SendEventAsync(message);
            }
        }

        internal async Task SendEventAsync(IEnumerable<Tuple<string, IDictionary<string, string>>> messages)
        {

            foreach (Tuple<string, IDictionary<string, string>> messageData in messages)
            {
                var message = new Message(Encoding.UTF8.GetBytes(messageData.Item1));
                await this.mqttIotHubAdapter.SendEventAsync(message, messageData.Item2);
            }
        }

        protected override async Task<Message> OnReceiveAsync(TimeSpan timeout)
        {
            return await this.mqttIotHubAdapter.ReceiveAsync(timeout);
        }

        protected override Task OnCompleteAsync(string lockToken)
        {
            throw new NotSupportedException();
        }

        protected override Task OnAbandonAsync(string lockToken)
        {
            throw new NotSupportedException();
        }

        protected override Task OnRejectAsync(string lockToken)
        {
            throw new NotSupportedException();
        }


        ISessionStatePersistenceProvider LoadPersistenceProvider(Settings settings)
        {
            //TODO: load from custom config setting section first otherwise use default impl
            throw new NotImplementedException();
        }

        Settings LoadSettings()
        {
            //TODO: load from custom config setting section first otherwise use default impl
            throw new NotImplementedException();
        }


        void ScheduleCleanup(Func<Task> cleanupTask)
        {
            Func<Task> currentCleanupFunc = this.cleanupFunc;
            this.cleanupFunc = async () =>
            {
                if (currentCleanupFunc != null)
                {
                    await currentCleanupFunc();
                }

                await cleanupTask();
            };
        }
    }
}