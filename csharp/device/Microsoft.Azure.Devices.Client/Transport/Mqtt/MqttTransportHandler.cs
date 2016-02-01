﻿// Copyright (c) Microsoft. All rights reserved.
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
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;

    sealed class MqttTransportHandler : TransportHandlerBase
    {
        const int ProtocolGatewayPort = 8883;

        readonly Bootstrap bootstrap;
        readonly IPAddress serverAddress;
        readonly TaskCompletionSource connectCompletion;
        readonly TaskCompletionSource disconnectCompletion;
        readonly ConcurrentQueue<MqttMessageReceivedResult> messageQueue;
        readonly Queue<string> completionQueue;
        readonly MqttIotHubAdapterFactory mqttIotHubAdapterFactory;
        readonly QualityOfService qos;
        readonly object syncRoot = new object();
        readonly SemaphoreSlim receivingSemaphore = new SemaphoreSlim(0);

        Func<Task> cleanupFunc;
        IChannel channel;

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString)
            : this(iotHubConnectionString, new MqttTransportSettings())
        {

        }

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings)
        {
            this.connectCompletion = new TaskCompletionSource();
            this.disconnectCompletion = new TaskCompletionSource();
            this.mqttIotHubAdapterFactory = new MqttIotHubAdapterFactory(settings);
            this.messageQueue = new ConcurrentQueue<MqttMessageReceivedResult>();
            this.completionQueue = new Queue<string>();
            this.serverAddress = Dns.GetHostEntry(iotHubConnectionString.HostName).AddressList[0];
            var group = new SingleInstanceEventLoopGroup();
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
                        this.mqttIotHubAdapterFactory.Create(
                            this.OnConnected,
                            this.OnDisconnected,
                            this.OnMessageReceived, iotHubConnectionString, settings));
                }));

            this.ScheduleCleanup(async () =>
            {
                if (!this.connectCompletion.Task.IsCompleted)
                {
                    this.connectCompletion.TrySetCanceled();
                }
                await group.ShutdownGracefullyAsync();
                this.disconnectCompletion.TryComplete();
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
                this.connectCompletion.TrySetException(ex);
            }
            this.ScheduleCleanup(async () =>
            {
                if (!this.disconnectCompletion.Task.IsFaulted)
                {
                    await this.channel.WriteAsync(DisconnectPacket.Instance);
                    await this.channel.CloseAsync();
                }
            });

            await this.connectCompletion.Task;
        }

        protected override async Task OnCloseAsync()
        {
            try
            {
                await this.cleanupFunc();
            }
            catch (Exception ex)
            {
                this.disconnectCompletion.TrySetException(ex);
            }
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

        protected override async Task<Message> OnReceiveAsync(TimeSpan timeout)
        {
            var cancellationToken = new CancellationToken();
            try
            {
                return await this.PeekAsync(cancellationToken).WithTimeout(timeout, () => "Receive message timed out.", cancellationToken);
            }
            catch (TimeoutException)
            {
                return null;
            }
            catch (ObjectDisposedException)
            {
                return null;
            }
        }

        async Task<Message> PeekAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await this.receivingSemaphore.WaitAsync(cancellationToken);
                MqttMessageReceivedResult receivedResult;
                lock (this.syncRoot)
                {
                    if (!this.messageQueue.TryDequeue(out receivedResult))
                    {
                        continue;
                    }
                    if (!receivedResult.Succeed)
                    {
                        throw receivedResult.Exception;
                    }
                    if (this.qos == QualityOfService.AtLeastOnce)
                    {
                        this.completionQueue.Enqueue(receivedResult.Message.LockToken);
                    }
                }
                return receivedResult.Message;
            }
        }

        protected override Task OnCompleteAsync(string lockToken)
        {
            if (this.qos == QualityOfService.AtMostOnce)
            {
                throw new InvalidOperationException("Complete is not allowed for QoS 0.");
            }

            lock (this.syncRoot)
            {
                if (this.completionQueue.Count == 0)
                {
                    throw new InvalidOperationException("Unknown lock token.");
                }
                if (this.completionQueue.Peek() == lockToken)
                {
                    this.completionQueue.Dequeue();
                }
                else
                {
                    throw new InvalidOperationException("Client MUST send PUBACK packets in the order in which the corresponding PUBLISH packets were received (QoS 1 messages) per [MQTT-4.6.0-2]");
                }
            }
            return this.channel.WriteAndFlushAsync(lockToken);
        }

        protected override Task OnAbandonAsync(string lockToken)
        {
            throw new NotSupportedException("MQTT protocol does not support this operation");
        }

        protected override Task OnRejectAsync(string lockToken)
        {
            throw new NotSupportedException("MQTT protocol does not support this operation");
        }

        void OnConnected(MqttActionResult actionResult)
        {
            if (actionResult.Succeed)
            {
                this.connectCompletion.TryComplete();
            }
            else
            {
                this.connectCompletion.TrySetException(actionResult.Exception);
            }
        }

        void OnDisconnected(MqttActionResult actionResult)
        {
            if (actionResult.Succeed)
            {
                this.disconnectCompletion.TryComplete();
            }
            else
            {
                this.disconnectCompletion.TrySetException(actionResult.Exception);
            }

            this.receivingSemaphore.Dispose();
        }

        void OnMessageReceived(MqttMessageReceivedResult actionResult)
        {
            if (actionResult.Succeed)
            {
                this.messageQueue.Enqueue(actionResult);
                this.receivingSemaphore.Release();
            }
            else
            {
                throw new IotHubException(actionResult.Exception);
            }
        }

        void ScheduleCleanup(Func<Task> cleanupTask)
        {
            Func<Task> currentCleanupFunc = this.cleanupFunc;
            this.cleanupFunc = async () =>
            {
                await cleanupTask();

                if (currentCleanupFunc != null)
                {
                    await currentCleanupFunc();
                }
            };
        }
    }

    class MqttActionResult
    {
        public bool Succeed
        {
            get { return this.Exception == null; }
        }

        public Exception Exception { get; set; }
    }

    class MqttMessageReceivedResult : MqttActionResult
    {
        public Message Message { get; set; }
    }
}