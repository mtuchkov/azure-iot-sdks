// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
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
    using Microsoft.Azure.Devices.Client.Common;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using TransportType = Microsoft.Azure.Devices.Client.TransportType;

    sealed class MqttTransportHandler : TransportHandler
    {
        const int ProtocolGatewayPort = 8883;

        [Flags]
        internal enum TransportState
        {
            NotInitialized = 1,
            Opening = 2,
            Open = 4,
            Subscribing = Open | 8,
            Receiving = Open | 16,
            Closed = 32,
            Error = 64
        }

        static readonly ConcurrentObjectPool<string, IEventLoopGroup> EventLoopGroupPool =
            new ConcurrentObjectPool<string, IEventLoopGroup>(Environment.ProcessorCount, TimeSpan.FromSeconds(5), elg => elg.ShutdownGracefullyAsync());
        
        readonly IPAddress serverAddress;
        readonly Func<IPAddress, int, Task<IChannel>> channelFactory;
        readonly ConcurrentQueue<string> completionQueue;
        readonly MqttIotHubAdapterFactory mqttIotHubAdapterFactory;
        readonly QualityOfService qos;

        readonly string eventLoopGroupKey;
        readonly object syncRoot = new object();
        readonly ConcurrentSpinWait receiveContentionSpinWait = new ConcurrentSpinWait();
        readonly CancellationTokenSource disconnectAwaitersCancellationSource = new CancellationTokenSource();
        readonly RetryPolicy closeRetryPolicy;

        readonly SemaphoreSlim receivingSemaphore = new SemaphoreSlim(0);
        readonly ConcurrentQueue<Message> messageQueue;

        readonly TaskCompletionSource connectCompletion = new TaskCompletionSource();
        readonly TaskCompletionSource subscribeCompletionSource = new TaskCompletionSource();
        Func<Task> cleanupFunc;
        IChannel channel;
        Exception fatalException;

        int state = (int)TransportState.NotInitialized;
        TransportState State => (TransportState)Volatile.Read(ref this.state);

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString)
            : this(iotHubConnectionString, new MqttTransportSettings(TransportType.Mqtt))
        {
            
        }

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings)
            : this(iotHubConnectionString, settings, null)
        {
        }

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings, Func<IPAddress, int, Task<IChannel>> channelFactory)
            :base(settings)
        {
            this.mqttIotHubAdapterFactory = new MqttIotHubAdapterFactory(settings);
            this.messageQueue = new ConcurrentQueue<Message>();
            this.completionQueue = new ConcurrentQueue<string>();
            this.serverAddress = Dns.GetHostEntry(iotHubConnectionString.HostName).AddressList[0];
            this.qos = settings.PublishToServerQoS;
            this.eventLoopGroupKey = iotHubConnectionString.IotHubName + "#" + iotHubConnectionString.DeviceId + "#" + iotHubConnectionString.Audience;
            this.channelFactory = channelFactory ?? this.CreateChannelFactory(iotHubConnectionString, settings);
            this.closeRetryPolicy = new RetryPolicy(new TransientErrorIgnoreStrategy(), 5, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
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
                throw new ArgumentNullException(nameof(hostname));
            }

            if (authMethod == null)
            {
                throw new ArgumentNullException(nameof(authMethod));
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
                throw new ArgumentNullException(nameof(connectionString));
            }

            IotHubConnectionString iotHubConnectionString = IotHubConnectionString.Parse(connectionString);

            return new MqttTransportHandler(iotHubConnectionString);
        }

        #region Client operations
        public override async Task OpenAsync(bool explicitOpen)
        {
            this.EnsureValidState();

            if (this.State == TransportState.Open)
            {
                return;
            }
            await this.OpenAsync();
        }

        public override async Task SendEventAsync(Message message)
        {
            this.EnsureValidState();

            await this.channel.WriteAndFlushAsync(message);
        }

        public override async Task SendEventAsync(IEnumerable<Message> messages)
        {
            foreach (Message message in messages)
            {
                await this.SendEventAsync(message);
            }
        }

        public override async Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            this.EnsureValidState();

            if (this.State != TransportState.Receiving)
            {
                await this.SubscribeAsync();
            }

            if (!await this.receivingSemaphore.WaitAsync(timeout, this.disconnectAwaitersCancellationSource.Token))
            {
                return null;
            }
            Message message;
            lock (this.syncRoot)
            {
                if (this.messageQueue.TryDequeue(out message))
                {
                    if (this.qos == QualityOfService.AtLeastOnce)
                    {
                        this.completionQueue.Enqueue(message.LockToken);
                    }
                }
                else
                {
                    throw new IotHubClientException("Message queue is empty");
                }
            }
            return message;
        }

        public override async Task CompleteAsync(string lockToken)
        {
            this.EnsureValidState();

            if (this.qos == QualityOfService.AtMostOnce)
            {
                throw new IotHubClientException(new InvalidOperationException("Complete is not allowed for QoS 0."));
            }

            bool raceCondition = false;
            Task completeOperationCompletion = null;
            string expectedLockToken;
            lock (this.syncRoot)
            {
                if (this.completionQueue.Count == 0)
                {
                    throw new IotHubClientException(new InvalidOperationException("Unknown lock token."));
                }
                if (!this.completionQueue.TryPeek(out expectedLockToken) || expectedLockToken != lockToken ||
                    !this.completionQueue.TryDequeue(out expectedLockToken) || expectedLockToken != lockToken)
                {
                    //exception will raise and spin will happen, so there is no a big performance penalty to try to find the token and tell to user what has happened
                    raceCondition = this.completionQueue.All(t => t != lockToken);
                }
                else
                {
                    completeOperationCompletion = this.channel.WriteAndFlushAsync(lockToken);
                }
            }
            if (raceCondition)
            {
                //We spin here to reduce contention and process the message that is on top of the queue first.
                this.receiveContentionSpinWait.SpinOnce();
                throw new IotHubClientTransientException(new InvalidOperationException($"Client MUST send PUBACK packets in the order in which the corresponding PUBLISH packets were received (QoS 1 messages) per [MQTT-4.6.0-2]. Expected lock token: '{expectedLockToken}'; actual lock token: '{lockToken}'."));
            }
            else if (completeOperationCompletion == null)
            {
                //Token is not found. It is either a garbage or token from previous session in any case we should not fail the channel - user should decide what to do
                throw new IotHubClientException("LockToken is not found. It is either not exist or this is a token from previous session. Do not retry operation");
            }
            else
            {
                this.receiveContentionSpinWait.Reset();
                await completeOperationCompletion;
            }
        }

        public override Task AbandonAsync(string lockToken)
        {
            throw new IotHubClientException(new NotSupportedException("MQTT protocol does not support this operation"));
        }

        public override Task RejectAsync(string lockToken)
        {
            throw new IotHubClientException(new NotSupportedException("MQTT protocol does not support this operation"));
        }

        public override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (this.TryStop())
                {
                    this.Cleanup();
                }
            }
        }

        public override async Task CloseAsync()
        {
            if (this.TryStop())
            {
                await this.closeRetryPolicy.ExecuteAsync(this.CleanupAsync);
            }
            else
            {
                if (this.State == TransportState.Error)
                {
                    throw new IotHubClientException(this.fatalException);
                }
            }
        }

        #endregion

        #region MQTT callbacks
        void OnConnected()
        {
            if (this.TryStateTransition(TransportState.Opening, TransportState.Open))
            {
                this.connectCompletion.TryComplete();
            }
        }

        void OnMessageReceived(Message message)
        {
            if ((this.State & TransportState.Open) > 0)
            {
                this.messageQueue.Enqueue(message);
                this.receivingSemaphore.Release();
            }
        }

        async void OnError(Exception exception)
        {
            this.fatalException = exception;
            try
            {
                TransportState previousState = this.MoveToStateIfPossible(TransportState.Error, TransportState.Closed);

                switch (previousState)
                {
                    case TransportState.Error:
                    case TransportState.Closed:
                        return;
                    case TransportState.NotInitialized:
                    case TransportState.Opening:
                        this.connectCompletion.TrySetException(exception);
                        this.subscribeCompletionSource.TrySetException(exception);
                        this.disconnectAwaitersCancellationSource.Cancel();
                        break;
                    case TransportState.Open:
                    case TransportState.Subscribing:
                        this.subscribeCompletionSource.TrySetException(exception);
                        this.disconnectAwaitersCancellationSource.Cancel();
                        break;
                    case TransportState.Receiving:
                        this.disconnectAwaitersCancellationSource.Cancel();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await this.closeRetryPolicy.ExecuteAsync(this.CleanupAsync);
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                
            }
        }

        TransportState MoveToStateIfPossible(TransportState destination, TransportState illigalStates)
        {
            TransportState previousState = this.State;
            do
            {
                if ((previousState & illigalStates) > 0)
                {
                    return destination;
                }
                TransportState prevState;
                if ((prevState = (TransportState)Interlocked.CompareExchange(ref this.state, (int)destination, (int)previousState)) == previousState)
                {
                    return prevState;
                }
                previousState = prevState;
            }
            while (true);
        }

        #endregion

        async Task OpenAsync()
        {
            if (this.TryStateTransition(TransportState.NotInitialized, TransportState.Opening))
            {
                try
                {
                    this.channel = await this.channelFactory(this.serverAddress, ProtocolGatewayPort);
                }
                catch (Exception ex) when(!ex.IsFatal())
                {
                    this.OnError(ex);
                    throw;
                }

                this.ScheduleCleanup(async () =>
                {
                    this.disconnectAwaitersCancellationSource.Cancel();
                    if (this.channel == null)
                    {
                        return;
                    }
                    if (this.channel.Active)
                    {
                        await this.channel.WriteAsync(DisconnectPacket.Instance);
                    }
                    if (this.channel.Open)
                    {
                        await this.channel.CloseAsync();
                    }
                });
            }

            await this.connectCompletion.Task;
        }

        bool TryStop()
        {
            TransportState previousState = this.MoveToStateIfPossible(TransportState.Closed, TransportState.Error);
            switch (previousState)
            {
                case TransportState.Closed:
                case TransportState.Error:
                    return false;
                case TransportState.NotInitialized:
                case TransportState.Opening:
                    this.connectCompletion.TrySetCanceled();
                    this.subscribeCompletionSource.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                    break;
                case TransportState.Open:
                case TransportState.Subscribing:
                    this.subscribeCompletionSource.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                    break;
                case TransportState.Receiving:
                    this.disconnectAwaitersCancellationSource.Cancel();
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return true;
        }

        async Task SubscribeAsync()
        {
            if (this.TryStateTransition(TransportState.Open, TransportState.Subscribing))
            {
                await this.channel.WriteAsync(new SubscribePacket());
                if (this.TryStateTransition(TransportState.Subscribing, TransportState.Receiving))
                {
                    if (!this.subscribeCompletionSource.TryComplete())
                    {
                        await this.subscribeCompletionSource.Task;
                    }
                }
            }
            await this.subscribeCompletionSource.Task;
        }

        Func<IPAddress, int, Task<IChannel>> CreateChannelFactory(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings)
        {
            return (address, port) =>
            {
                IEventLoopGroup eventLoopGroup = EventLoopGroupPool.TakeOrAdd(this.eventLoopGroupKey, () => new MultithreadEventLoopGroup(() => new SingleThreadEventLoop("MQTTExecutionThread", TimeSpan.FromSeconds(1)), 1));
                Bootstrap bootstrap = new Bootstrap().Group(eventLoopGroup).Channel<TcpSocketChannel>().Option(ChannelOption.TcpNodelay, true).Handler(new ActionChannelInitializer<ISocketChannel>(ch => { ch.Pipeline.AddLast(TlsHandler.Client(iotHubConnectionString.HostName, null, (sender, certificate, chain, errors) => true), MqttEncoder.Instance, new MqttDecoder(false, 256 * 1024), this.mqttIotHubAdapterFactory.Create(this.OnConnected, this.OnMessageReceived, this.OnError, iotHubConnectionString, settings)); }));

                this.ScheduleCleanup(() =>
                {
                    EventLoopGroupPool.Release(this.eventLoopGroupKey);
                    return TaskConstants.Completed;
                });

                return bootstrap.ConnectAsync(address, port);
            };
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

        async void Cleanup()
        {
            try
            {
                await this.closeRetryPolicy.ExecuteAsync(this.CleanupAsync);
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
            }
        }

        Task CleanupAsync()
        {
            if (this.cleanupFunc != null)
            {
                return this.cleanupFunc();
            }
            return TaskConstants.Completed;
        }

        bool TryStateTransition(TransportState fromState, TransportState toState)
        {
            return (TransportState)Interlocked.CompareExchange(ref this.state, (int)toState, (int)fromState) == fromState;
        }

        void EnsureValidState()
        {
            if (this.State == TransportState.Error)
            {
                throw new IotHubClientException(this.fatalException);
            }
            if (this.State == TransportState.Closed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }
    }
}