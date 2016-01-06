// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DotNetty.Codecs.Mqtt;
    using DotNetty.Codecs.Mqtt.Packets;
    using DotNetty.Common.Concurrency;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Bootstrapping;
    using DotNetty.Transport.Channels;
    using DotNetty.Transport.Channels.Sockets;
    using Microsoft.Azure.Devices.Client.Common;
    using Microsoft.Azure.Devices.Client.Extensions;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    sealed class MqttTransportHandler : TansportHandlerBase
    {
        const int ProtocolGatewayPort = 8883;
        static readonly ImplementationLoader typeLoader = new ImplementationLoader();
        
        readonly Bootstrap bootstrap;
        readonly IPAddress serverAddress;
        readonly TaskCompletionSource connectCompletion;
        readonly TaskCompletionSource disconnectCompletion;
        readonly ConcurrentQueue<Message> readyQueue;
        readonly List<string> completedMessages;
        readonly Queue<string> readQueue;
        readonly MqttIotHubAdapterFactory mqttIotHubAdapterFactory;
        readonly QualityOfService qos;
        Func<Task> cleanupFunc;
        IChannel channel;
        readonly SemaphoreSlim semaphor = new SemaphoreSlim(1, 1);

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString)
        {
            this.connectCompletion = new TaskCompletionSource();
            this.disconnectCompletion = new TaskCompletionSource();
            this.mqttIotHubAdapterFactory = new MqttIotHubAdapterFactory(typeLoader);
            this.readyQueue = new ConcurrentQueue<Message>();
            this.readQueue = new Queue<string>();
            this.completedMessages = new List<string>();
            this.serverAddress = Dns.GetHostEntry(iotHubConnectionString.HostName).AddressList[0];
            var group = new SingleInstanceEventLoopGroup();
            var settings = new Settings(typeLoader.LoadImplementation<ISettingsProvider>());
            this.qos = settings.PublishToServerQoS;

            this.bootstrap = new Bootstrap()
                .Group(@group)
                .Channel<TcpSocketChannel>()
                .Option(ChannelOption.TcpNodelay, true)
                .Handler(new ActionChannelInitializer<ISocketChannel>(ch =>
                {
                    ch.Pipeline.AddLast(
                        TlsHandler.Client(iotHubConnectionString.HostName, null),
                        MqttEncoder.Instance,
                        new MqttDecoder(false, 256 * 1024),
                        this.mqttIotHubAdapterFactory.Create(this.OnConnected, this.OnDisconnected, iotHubConnectionString, settings, this.readyQueue));
                }));

            this.ScheduleCleanup(async () =>
            {
                if (!this.connectCompletion.Task.IsCompleted)
                {
                    this.connectCompletion.SetCanceled();
                }
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
            try
            {
                this.channel = await this.bootstrap.ConnectAsync(this.serverAddress, ProtocolGatewayPort);
            }
            catch (Exception ex)
            {
                if (ex.IsFatal())
                {
                    throw;
                }
                this.connectCompletion.SetException(ex);
            }
            this.ScheduleCleanup(async () =>
            {
                await this.channel.CloseAsync();
            });

            await this.connectCompletion.Task;
        }

        protected override async Task OnCloseAsync()
        {
            await this.cleanupFunc();
            await this.disconnectCompletion.Task;
        }

        protected override Task OnSendEventAsync(Message message)
        {
            return this.channel.WriteAndFlushAsync(message);
        }

        protected override async Task OnSendEventAsync(IEnumerable<Message> messages)
        {
            foreach (Message message in messages)
            {
                await this.SendEventAsync(message);
            }
        }

        internal async Task SendEventAsync(IEnumerable<string> messages)
        {
            foreach (string messageString in messages)
            {
                var message = new Message(Encoding.UTF8.GetBytes(messageString));
                await this.SendEventAsync(message);
            }
        }

        internal async Task SendEventAsync(IEnumerable<Tuple<string, IDictionary<string, string>>> messages)
        {
            foreach (Tuple<string, IDictionary<string, string>> messageData in messages)
            {
                var message = new Message(Encoding.UTF8.GetBytes(messageData.Item1));
                foreach (KeyValuePair<string, string> property in messageData.Item2)
                {
                    message.Properties.Add(property);
                }
                await this.SendEventAsync(message);
            }
        }

        protected override Task<Message> OnReceiveAsync(TimeSpan timeout)
        {
            Func<Task<Message>> asyncOperation = this.PeekAsync;
            return asyncOperation.WithTimeoutAsync(timeout);
        }

        async Task<Message> PeekAsync()
        {
            Message message;
            while (!this.readyQueue.TryDequeue(out message))
            {
                await Task.Delay(100);
            }

            await this.semaphor.WaitAsync();
            this.readQueue.Enqueue(message.LockToken);
            this.semaphor.Release();
            
            return message;
        }

        protected override async Task OnCompleteAsync(string lockToken)
        {
            if (this.qos == QualityOfService.AtMostOnce)
            {
                return;
            }
            
            await this.semaphor.WaitAsync();
            this.completedMessages.Add(lockToken);
            await this.TryAcknowledgeAsync();
            
            this.semaphor.Release();
        }

        async Task TryAcknowledgeAsync()
        {
            while (this.completedMessages.Contains(this.readQueue.Peek()))
            {
                string lockToken = this.readQueue.Dequeue();
                this.completedMessages.Remove(lockToken);
                await this.channel.WriteAndFlushAsync(new PubAckPacket());
            }
        }

        protected override Task OnAbandonAsync(string lockToken)
        {
            throw new NotSupportedException();
        }

        protected override Task OnRejectAsync(string lockToken)
        {
            throw new NotSupportedException();
        }

        void OnConnected()
        {
            this.connectCompletion.Complete();
        }

        void OnDisconnected()
        {
            this.disconnectCompletion.Complete();
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

    class MqttIotHubAdapterFactory 
    {
        readonly ImplementationLoader typeLoader;

        public MqttIotHubAdapterFactory(ImplementationLoader typeLoader)
        {
            this.typeLoader = typeLoader;
        }

        public MqttIotHubAdapter Create(
            Action onConnected, Action 
            onDisconnected, 
            IotHubConnectionString iotHubConnectionString, 
            Settings settings, 
            ConcurrentQueue<Message> receivedMessageQueue)
        {
            var persistanceProvider = this.typeLoader.LoadImplementation<ISessionStatePersistenceProvider>();
            var willMessageProvider = this.typeLoader.LoadImplementation<IWillMessageProvider>();
            var topicNameRouter = this.typeLoader.LoadImplementation<ITopicNameRouter>();
            var sessionContextProvider = this.typeLoader.LoadImplementation<ISessionContextProvider>();

            return new MqttIotHubAdapter(
                iotHubConnectionString.DeviceId,
                iotHubConnectionString.HostName,
                iotHubConnectionString.GetPassword(),
                settings,
                persistanceProvider,
                sessionContextProvider,
                topicNameRouter,
                willMessageProvider,
                onConnected,
                onDisconnected,
                receivedMessageQueue);
        }
    }
}