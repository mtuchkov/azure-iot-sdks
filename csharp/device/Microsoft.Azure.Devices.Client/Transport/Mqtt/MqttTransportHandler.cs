// Copyright (c) Microsoft. All rights reserved.
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
    using Microsoft.Azure.Devices.Client.Common;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using TransportType = Microsoft.Azure.Devices.Client.TransportType;

    sealed class MqttTransportHandler : TransportHandlerBase
    {
        const int ProtocolGatewayPort = 8883;

        [Flags]
        enum StateFlags: long
        {
            NotInitialized = 1,
            Opening = 2,
            Open = 4,
            Subscribing = Open | 8,
            Receiving = Open | 16,
            Closing = 32,
            Closed = 64,
            Disposed = 128,
            Inconclusive = 256,
            Fatal = 512,
        }

        static readonly TimeSpan DefaultReceiveTimeoutInSeconds = TimeSpan.FromMinutes(1);
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

        SemaphoreSlim receivingSemaphore = new SemaphoreSlim(0);
        Exception fatalException;
        Exception transientException;
        TaskCompletionSource connectCompletion = new TaskCompletionSource();
        TaskCompletionSource resetConnectionCompletion = new TaskCompletionSource();
        TaskCompletionSource subscribeCompletionSource = new TaskCompletionSource();
        long transportStatus = (long)StateFlags.NotInitialized;
        int generation;

        ConcurrentQueue<Message> messageQueue;
        Func<Task> cleanupFunc;
        IChannel channel;
        
        StateFlags State
        {
            get { return (StateFlags)Interlocked.Read(ref this.transportStatus); }
            set { Interlocked.Exchange(ref this.transportStatus, (long)value); }
        }

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString)
            : this(iotHubConnectionString, new MqttTransportSettings(TransportType.Mqtt))
        {
        }

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings)
            : this(iotHubConnectionString, settings, null)
        {
        }

        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings, Func<IPAddress, int, Task<IChannel>> channelFactory)
        {
            this.DefaultReceiveTimeout = DefaultReceiveTimeoutInSeconds;
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

        protected override TimeSpan DefaultReceiveTimeout { get; set; }

        #region Client operations
        protected override async Task OnOpenAsync(bool explicitOpen)
        {
            if (await this.TryOpenAsync())
            {
                return;
            }

            await this.HandleInvalidState(this.State, false);
        }

        protected override async Task OnCloseAsync()
        {
            await this.CloseAsync(Volatile.Read(ref this.generation), null);
        }

        protected override async Task OnSendEventAsync(Message message)
        {
            await this.EnsureWorkingStatesAsync(true);

            await this.channel.WriteAndFlushAsync(message);
        }

        protected override async Task OnSendEventAsync(IEnumerable<Message> messages)
        {
            await this.EnsureWorkingStatesAsync(true);

            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }
            foreach (Message message in messages)
            {
                await this.SendEventAsync(message);
            }
        }

        protected override void Dispose(bool disposing)
        {
            StateFlags previousState = this.AtomicStatusChange(StateFlags.Disposed);
            if (disposing)
            {
                if (previousState == StateFlags.NotInitialized || previousState == StateFlags.Opening)
                {
                    this.connectCompletion.TrySetCanceled();
                    this.subscribeCompletionSource.TrySetCanceled();
                    this.resetConnectionCompletion.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                }
                else if (previousState == StateFlags.Open || previousState == StateFlags.Subscribing)
                {
                    this.subscribeCompletionSource.TrySetCanceled();
                    this.resetConnectionCompletion.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                }
                else if (previousState == StateFlags.Receiving)
                {
                    this.resetConnectionCompletion.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                }
                else if (previousState == StateFlags.Inconclusive)
                {
                    this.connectCompletion.TrySetCanceled();
                    this.subscribeCompletionSource.TrySetCanceled();
                    this.resetConnectionCompletion.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                }
                else if ((previousState & (StateFlags.Closing | StateFlags.Closed | StateFlags.Fatal | StateFlags.Disposed)) > 0)
                {
                    return;
                }
                this.Cleanup();
            }
        }

        protected override async Task<Message> OnReceiveAsync(TimeSpan timeout)
        {
            await this.EnsureWorkingStatesAsync(true);

            if (!await this.TrySubscribeAsync())
            {
                await this.HandleInvalidState(this.State, false);
            }

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

        protected override async Task OnCompleteAsync(string lockToken)
        {
            await this.EnsureWorkingStatesAsync(false);

            if (this.qos == QualityOfService.AtMostOnce)
            {
                throw new InvalidOperationException("Complete is not allowed for QoS 0.");
            }

            bool raceCondition = false;
            Task completeOperationCompletion = null;
            string expectedLockToken;
            lock (this.syncRoot)
            {
                if (this.completionQueue.Count == 0)
                {
                    throw new InvalidOperationException("Unknown lock token.");
                }
                if (!this.completionQueue.TryPeek(out expectedLockToken) || expectedLockToken != lockToken ||
                    !this.completionQueue.TryDequeue(out expectedLockToken) || expectedLockToken != lockToken)
                {
                    raceCondition = true;
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
                throw new InvalidOperationException($"Client MUST send PUBACK packets in the order in which the corresponding PUBLISH packets were received (QoS 1 messages) per [MQTT-4.6.0-2]. Expected lock token: '{expectedLockToken}'; actual lock token: '{lockToken}'.");
            }
            else
            {
                this.receiveContentionSpinWait.Reset();
                await completeOperationCompletion;
            }
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
        void OnConnected(long channelGeneration)
        {
            TaskCompletionSource connectCompletionSource = this.connectCompletion;
            if (this.TryStateTransition(StateFlags.Opening, StateFlags.Open))
            {
                if (connectCompletionSource.TryComplete())
                {
                    Interlocked.CompareExchange(ref this.connectCompletion, new TaskCompletionSource(), connectCompletionSource);
                }
            }
        }

        void OnMessageReceived(long channelGeneration, Message message)
        {
            if (Volatile.Read(ref this.generation) == channelGeneration)
            {
                this.messageQueue.Enqueue(message);
                this.receivingSemaphore.Release();
            }
        }

        async void OnError(long channelGeneration, Exception exception)
        {
            //Console.WriteLine(exception.ToString());
            await this.CloseAsync(channelGeneration, exception);
        }

        #endregion

        async Task<bool> TryOpenAsync()
        {
            Task<int> currentConnectCompletion = this.connectCompletion.Task;
            if (this.TryStateTransition(StateFlags.NotInitialized, StateFlags.Opening))
            {
                int channelGeneration = Volatile.Read(ref this.generation);
                try
                {
                    this.channel = await this.channelFactory(this.serverAddress, ProtocolGatewayPort);
                }
                catch (Exception ex)
                {
                    if (ex.IsFatal())
                    {
                        throw;
                    }

                    this.OnError(channelGeneration, ex);
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

                await currentConnectCompletion;
                return true;
            }
            else
            {
                if (this.TryStateTransition(StateFlags.Opening, StateFlags.Opening))
                {
                    await currentConnectCompletion;
                    return true;
                }
                return (this.State & StateFlags.Open) > 0;
            }
        }

        async Task CloseAsync(long channelGeneration, Exception exception)
        {
            var initialCompletionSources = new[] { this.connectCompletion, this.subscribeCompletionSource, this.resetConnectionCompletion };
            try
            {
                if (this.TryStateTransition(StateFlags.Opening | StateFlags.Open | StateFlags.Subscribing | StateFlags.Receiving, StateFlags.Inconclusive))
                {
                    int localGeneration = this.generation;
                    if (Interlocked.CompareExchange(ref this.generation, localGeneration + 1, localGeneration) == channelGeneration)
                    {
                        this.transientException = exception;
                        await this.ResetAsync(exception);
                    }
                }
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                this.Abort(initialCompletionSources, exception ?? ex);
            }
        }
        
        async Task<bool> TrySubscribeAsync()
        {
            if (this.TryStateTransition(StateFlags.Open, StateFlags.Subscribing))
            {
                try
                {
                    await this.channel.WriteAsync(new SubscribePacket());
                    TaskCompletionSource subscribeCompletion = this.subscribeCompletionSource;
                    if (this.TryStateTransition(StateFlags.Subscribing, StateFlags.Receiving))
                    {
                        if (subscribeCompletion.TryComplete())
                        {
                            Interlocked.CompareExchange(ref this.subscribeCompletionSource, new TaskCompletionSource(), subscribeCompletion);
                        }
                    }
                }
                catch (Exception ex)
                {
                    this.OnError(Volatile.Read(ref this.generation), ex);
                }
                return true;
            }
            else
            {
                Task<int> currentSubscribeCompletion = this.subscribeCompletionSource.Task;
                if (this.TryStateTransition(StateFlags.Subscribing, StateFlags.Subscribing))
                {
                    await currentSubscribeCompletion;
                    return true;
                }
                return this.State == StateFlags.Receiving;
            }
        }

        void Abort(IEnumerable<TaskCompletionSource> completionSources, Exception exception)
        {
            foreach (TaskCompletionSource taskCompletionSource in completionSources)
            {
                taskCompletionSource?.TrySetException(exception);
            }
            this.connectCompletion?.TrySetException(exception);
            this.subscribeCompletionSource?.TrySetException(exception);
            this.resetConnectionCompletion?.TrySetException(exception);
            this.fatalException = exception;
            this.State = StateFlags.Fatal;
        }

        async Task ResetAsync(Exception exception = null)
        {
            if (exception != null)
            {
                this.connectCompletion.TrySetException(exception);
                this.subscribeCompletionSource.TrySetException(exception);
            }
            else
            {
                this.connectCompletion.TrySetCanceled();
                this.subscribeCompletionSource.TrySetCanceled();
            }

            Interlocked.Exchange(ref this.connectCompletion, new TaskCompletionSource());
            Interlocked.Exchange(ref this.subscribeCompletionSource, new TaskCompletionSource());

            await this.closeRetryPolicy.ExecuteAsync(this.CleanupAsync);
            this.cleanupFunc = null;
            Interlocked.Exchange(ref this.messageQueue, new ConcurrentQueue<Message>());
            this.receivingSemaphore.Dispose();
            Interlocked.Exchange(ref this.receivingSemaphore, new SemaphoreSlim(0));
            TaskCompletionSource resetCompletionSource = this.resetConnectionCompletion;
            if (this.TryStateTransition(StateFlags.Inconclusive, StateFlags.NotInitialized))
            {
                if (resetCompletionSource.TryComplete())
                {
                    Interlocked.CompareExchange(ref this.resetConnectionCompletion, new TaskCompletionSource(), resetCompletionSource);
                }
            }
        }

        StateFlags AtomicStatusChange(StateFlags newState)
        {
            StateFlags oldState = this.State;
            var stateArray = new[] { oldState };
            SpinWait.SpinUntil(() => (stateArray[0] = (StateFlags)Interlocked.CompareExchange(ref this.transportStatus, (long)newState, (long)stateArray[0])) != newState);
            return stateArray[0];
        }

        Func<IPAddress, int, Task<IChannel>> CreateChannelFactory(IotHubConnectionString iotHubConnectionString, MqttTransportSettings settings)
        {
            return (address, port) =>
            {
                IEventLoopGroup eventLoopGroup = EventLoopGroupPool.TakeOrAdd(this.eventLoopGroupKey,
                () => new MultithreadEventLoopGroup(() => new SingleThreadEventLoop("MQTTExecutionThread", TimeSpan.FromSeconds(1)), 1));
                int channelGeneration = Volatile.Read(ref this.generation);

                Bootstrap bootstrap = new Bootstrap()
                    .Group(eventLoopGroup)
                    .Channel<TcpSocketChannel>()
                    .Option(ChannelOption.TcpNodelay, true)
                    .Handler(new ActionChannelInitializer<ISocketChannel>(ch =>
                    {
                        ch.Pipeline.AddLast(
                            TlsHandler.Client(iotHubConnectionString.HostName, null, (sender, certificate, chain, errors) => true),
                            MqttEncoder.Instance,
                            new MqttDecoder(false, 256 * 1024),
                            this.mqttIotHubAdapterFactory.Create(
                                channelGeneration,
                                this.OnConnected,
                                this.OnMessageReceived,
                                this.OnError,
                                iotHubConnectionString,
                                settings));
                    }));

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

        bool TryStateTransition(StateFlags fromState, StateFlags toState)
        {
            foreach (StateFlags state in new[] { StateFlags.NotInitialized, StateFlags.Opening, StateFlags.Open, StateFlags.Subscribing, StateFlags.Receiving, StateFlags.Inconclusive, StateFlags.Closing, StateFlags.Closed, StateFlags.Disposed, StateFlags.Fatal })
            {
                if ((state & fromState) > 0)
                {
                    if ((StateFlags)Interlocked.CompareExchange(ref this.transportStatus, (long)toState, (long)state) == state)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        async Task EnsureWorkingStatesAsync(bool reopen)
        {
            StateFlags state = this.State;

            if ((state & (StateFlags.NotInitialized | StateFlags.Opening)) > 0)
            {
                await this.OnOpenAsync(true);
            }

            if ((state & (StateFlags.Open | StateFlags.Subscribing | StateFlags.Receiving)) > 0)
            {
                return;
            }

            await this.HandleInvalidState(state, reopen);
        }

        async Task HandleInvalidState(StateFlags state, bool reopen)
        {
            if (state == StateFlags.Disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }

            if (state == StateFlags.Inconclusive)
            {
                if (!reopen)
                {
                    throw new IotHubClientException(this.transientException);
                }

                Task<int> currentResetCompletion = this.resetConnectionCompletion.Task;
                if (this.TryStateTransition(StateFlags.Inconclusive, StateFlags.Inconclusive))
                {
                    await currentResetCompletion;
                }

                if (!await this.TryOpenAsync())
                {
                    await this.HandleInvalidState(this.State, false);
                }
            }

            if (state == StateFlags.Fatal)
            {
                throw new IotHubClientException(this.fatalException);
            }

            throw new IotHubCommunicationException("Connection is closed. Please re-try the operation.");
        }
    }
}