﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using DotNetty.Codecs.Mqtt;
    using DotNetty.Codecs.Mqtt.Packets;
    using DotNetty.Common.Concurrency;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Bootstrapping;
    using DotNetty.Transport.Channels;
    using DotNetty.Transport.Channels.Sockets;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;
    using TransportType = Microsoft.Azure.Devices.Client.TransportType;

    sealed class MqttTransportHandler : TransportHandlerBase
    {
        const int ProtocolGatewayPort = 8883;

        [Flags]
        enum StateFlags: long
        {
            Closed = 0,
            Open = 1,
            Closing = 2,
            Error = 4,
        }

        readonly Bootstrap bootstrap;
        readonly IPAddress serverAddress;
        readonly TaskCompletionSource connectCompletion;
        readonly ConcurrentQueue<Message> messageQueue;
        readonly Queue<string> completionQueue;
        readonly MqttIotHubAdapterFactory mqttIotHubAdapterFactory;
        readonly QualityOfService qos;
        readonly object syncRoot = new object();
        readonly SemaphoreSlim receivingSemaphore = new SemaphoreSlim(0);
        readonly CancellationTokenSource disconnectAwaitersCancellationSource = new CancellationTokenSource();

        StateFlags State
        {
            get { return (StateFlags)Interlocked.Read(ref this.transportStatus); }
            set { Interlocked.Exchange(ref this.transportStatus, (long)value); }
        }

        Func<Task> cleanupFunc;
        IChannel channel;
        long transportStatus = (long)StateFlags.Closed;
        Exception transportException;
        public static readonly TimeSpan DefaultReceiveTimeoutInSeconds = TimeSpan.FromSeconds(20);

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString)
            : this(iotHubConnectionString, new MqttTransportSettings(TransportType.Mqtt))
        {

        }

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings)
        {
            this.DefaultReceiveTimeout = DefaultReceiveTimeoutInSeconds;
            this.connectCompletion = new TaskCompletionSource();
            this.mqttIotHubAdapterFactory = new MqttIotHubAdapterFactory(settings);
            this.messageQueue = new ConcurrentQueue<Message>();
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
                            this.OnMessageReceived,
                            this.OnError, 
                            iotHubConnectionString, 
                            settings));
                }));

            this.ScheduleCleanup(async () =>
            {
                if (!this.connectCompletion.Task.IsCompleted)
                {
                    this.connectCompletion.TrySetCanceled();
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

        #region Client oprtations
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
                this.disconnectAwaitersCancellationSource.Cancel();
                await this.channel.WriteAsync(DisconnectPacket.Instance);
                await this.channel.CloseAsync();
            });

            await this.connectCompletion.Task;
            this.State = StateFlags.Open;
        }

        protected override async Task OnCloseAsync()
        {
            this.ThrowIfNotOpen();
            try
            {
                this.State = StateFlags.Closing;
                await this.cleanupFunc();
                this.State = StateFlags.Closed;
            }
            catch (Exception ex)
            {
                this.transportException = ex;
                this.State = StateFlags.Error;
            }
        }

        protected override Task OnSendEventAsync(Message message)
        {
            this.ThrowIfNotOpen();

            return this.channel.WriteAndFlushAsync(message).ContinueWith((t, s) => { }, TaskScheduler.Default);
        }

        protected override async Task OnSendEventAsync(IEnumerable<Message> messages)
        {
            this.ThrowIfNotOpen();

            foreach (Message message in messages)
            {
                await this.SendEventAsync(message);
            }
        }

        protected override async Task<Message> OnReceiveAsync(TimeSpan timeout)
        {
            this.ThrowIfNotOpen();
            try
            {
                await this.receivingSemaphore.WaitAsync(timeout, this.disconnectAwaitersCancellationSource.Token);
                Message message;
                lock (this.syncRoot)
                {
                    if (!this.messageQueue.TryDequeue(out message))
                    {
                        return null;
                    }
                    if (this.qos == QualityOfService.AtLeastOnce)
                    {
                        this.completionQueue.Enqueue(message.LockToken);
                    }
                }
                return message;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        protected override Task OnCompleteAsync(string lockToken)
        {
            this.ThrowIfNotOpen();
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
                string expectedLockToken = this.completionQueue.Peek();
                if (expectedLockToken == lockToken)
                {
                    this.completionQueue.Dequeue();
                }
                else
                {
                    throw new InvalidOperationException($"Client MUST send PUBACK packets in the order in which the corresponding PUBLISH packets were received (QoS 1 messages) per [MQTT-4.6.0-2]. Expected lock token: '{expectedLockToken}'; actual lock token: '{lockToken}'.");
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

        #endregion

        #region MQTT callbacks
        void OnConnected()
        {
            this.connectCompletion.TryComplete();
        }

        void OnMessageReceived(Message message)
        {
            this.messageQueue.Enqueue(message);
            this.receivingSemaphore.Release();
        }

        async void OnError(Exception exception)
        {
            this.transportException = exception;

            try
            {
                await this.OnCloseAsync();
            }
            catch (Exception ex)
            {
                this.transportException = new AggregateException(this.transportException, ex);
            }
        }
        #endregion

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

        void ThrowIfNotOpen()
        {
            var state = this.State;

            if (state != StateFlags.Open)
            {
                if (state == StateFlags.Error)
                {
                    throw this.transportException;
                }

                throw new IotHubCommunicationException("Connection is closed.");
            }
        }
    }
}