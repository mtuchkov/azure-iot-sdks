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

    sealed class TransientErrorIgnoreStrategy : ITransientErrorDetectionStrategy
    {
        /// <summary>
        /// Always returns false.
        /// 
        /// </summary>
        /// <param name="ex">The exception.</param>
        /// <returns>
        /// Always false.
        /// </returns>
        public bool IsTransient(Exception ex)
        {
            return !ex.IsFatal();
        }
    }

    sealed class MqttTransportHandler : TransportHandlerBase
    {
        const int ProtocolGatewayPort = 8883;

        [Flags]
        enum StateFlags: long
        {
            NotInitialized = 1,
            Opening = 2,
            Open = 4,
            Subscribing = 8,
            Receiving = 16,
            Closing = 32,
            Closed = 64,
            Disposed = 128,
            TransientError = 256,
            Fatal = 512,
            InvalidStates = Closing | Closed | Disposed | Fatal
        }

        static readonly ConcurrentObjectPool<string, IEventLoopGroup> EventLoopGroupPool =
            new ConcurrentObjectPool<string, IEventLoopGroup>(Environment.ProcessorCount, TimeSpan.FromSeconds(5), elg => elg.ShutdownGracefullyAsync());
        
        static readonly TimeSpan DefaultReceiveTimeoutInSeconds = TimeSpan.FromMinutes(1);

        readonly Func<IPAddress, int, Task<IChannel>> channelFactory;
        readonly string eventLoopGroupKey;
        readonly IPAddress serverAddress;
        ConcurrentQueue<Message> messageQueue;
        readonly ConcurrentQueue<string> completionQueue;
        readonly MqttIotHubAdapterFactory mqttIotHubAdapterFactory;
        readonly QualityOfService qos;
        readonly object syncRoot = new object();
        SemaphoreSlim receivingSemaphore = new SemaphoreSlim(0);
        readonly CancellationTokenSource disconnectAwaitersCancellationSource = new CancellationTokenSource();
        TaskCompletionSource connectCompletion = new TaskCompletionSource();
        TaskCompletionSource resetConnectionCompletion = new TaskCompletionSource();
        TaskCompletionSource subscribeCompletionSource = new TaskCompletionSource();

        StateFlags State
        {
            get { return (StateFlags)Interlocked.Read(ref this.transportStatus); }
            set { Interlocked.Exchange(ref this.transportStatus, (long)value); }
        }

        Func<Task> cleanupFunc;
        IChannel channel;
        long transportStatus = (long)StateFlags.NotInitialized;
        readonly RetryPolicy closeRetryPolicy;
        Exception fatalException;
        int currentRetryCount;
        readonly int maxRetryCount = 3;
        readonly TimeSpan minBackoff = TimeSpan.FromMilliseconds(100);
        readonly TimeSpan deltaBackoff = TimeSpan.FromMilliseconds(100);
        readonly TimeSpan maxBackoff = TimeSpan.FromSeconds(10);
        Exception transientException;

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
            new RetryPolicy(new TransientErrorIgnoreStrategy(), 10, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
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
            StateFlags previousState = AtomicStatusChange(StateFlags.Closing);
            if ((previousState & StateFlags.Opening | StateFlags.Open | StateFlags.Subscribing | StateFlags.Receiving) > 0)
            {
                await this.StopAsync();
            }
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
            StateFlags previousState = AtomicStatusChange(StateFlags.Disposed);
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
                else if (previousState == StateFlags.TransientError)
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

            lock (this.syncRoot)
            {
                if (this.completionQueue.Count == 0)
                {
                    throw new InvalidOperationException("Unknown lock token.");
                }
                string expectedLockToken;
                if (!this.completionQueue.TryPeek(out expectedLockToken) || expectedLockToken != lockToken || 
                    !this.completionQueue.TryDequeue(out expectedLockToken) || expectedLockToken != lockToken)
                {
                    throw new InvalidOperationException($"Client MUST send PUBACK packets in the order in which the corresponding PUBLISH packets were received (QoS 1 messages) per [MQTT-4.6.0-2]. Expected lock token: '{expectedLockToken}'; actual lock token: '{lockToken}'.");
                }
            }
            await this.channel.WriteAndFlushAsync(lockToken);
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
            if (TryStateTransition(StateFlags.Opening, StateFlags.Open))
            {
                this.connectCompletion.TryComplete();
                Interlocked.Exchange(ref this.connectCompletion, new TaskCompletionSource());
            }
        }

        void OnMessageReceived(Message message)
        {
            this.messageQueue.Enqueue(message);
            this.receivingSemaphore.Release();
        }

        async void OnError(Exception exception)
        {
            this.transientException = exception;
            var initialCompletionSources = new[] { this.connectCompletion, this.subscribeCompletionSource, this.resetConnectionCompletion };
            try
            {
                if (this.TryStateTransition(StateFlags.Opening | StateFlags.Open | StateFlags.Subscribing | StateFlags.Receiving, StateFlags.TransientError))
                {
                    this.connectCompletion.TrySetException(exception);
                    Interlocked.Exchange(ref this.connectCompletion, new TaskCompletionSource());
                    this.subscribeCompletionSource.TrySetException(exception);
                    Interlocked.Exchange(ref this.subscribeCompletionSource, new TaskCompletionSource());
                    await this.ResetAsync();
                }
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                this.Abort(initialCompletionSources, new AggregateException(exception, ex));
            }
        }

        #endregion

        async Task<bool> TryOpenAsync()
        {
            if (this.TryStateTransition(StateFlags.NotInitialized, StateFlags.Opening))
            {
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

                    this.OnError(ex);
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

                await this.connectCompletion.Task;
                return true;
            }
            else
            {
                Task<int> currentConnectCompletion = this.connectCompletion.Task;
                if (this.TryStateTransition(StateFlags.Opening, StateFlags.Opening))
                {
                    await currentConnectCompletion;
                    return true;
                }
                return this.State == StateFlags.Open;
            }
        }

        async Task StopAsync()
        {
            try
            {
                await this.closeRetryPolicy.ExecuteAsync(this.CleanupAsync);
                this.connectCompletion.TrySetCanceled();
                this.subscribeCompletionSource.TrySetCanceled();
                this.resetConnectionCompletion.TrySetCanceled();
                this.TryStateTransition(StateFlags.Closing, StateFlags.Closed);
            }
            catch (Exception ex)
            {
                this.Abort(new[] { this.connectCompletion, this.subscribeCompletionSource, this.resetConnectionCompletion }, ex);
                this.State = StateFlags.Fatal;
                throw;
            }
        }

        async Task<bool> TrySubscribeAsync()
        {
            if (this.TryStateTransition(StateFlags.Open, StateFlags.Subscribing))
            {
                try
                {
                    await this.channel.WriteAsync(new SubscribePacket());
                    if (this.TryStateTransition(StateFlags.Subscribing, StateFlags.Receiving))
                    {
                        this.subscribeCompletionSource.TryComplete();
                        Interlocked.Exchange(ref this.subscribeCompletionSource, new TaskCompletionSource());
                    }
                }
                catch (Exception ex)
                {
                    this.OnError(ex);
                }
                return true;
            }
            else
            {
                Task<int> currentSubscribeCompletion = this.subscribeCompletionSource.Task;
                if (TryStateTransition(StateFlags.Subscribing, StateFlags.Subscribing))
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

        async Task ResetAsync()
        {
            await this.closeRetryPolicy.ExecuteAsync(this.CleanupAsync);
            this.cleanupFunc = null;
            Interlocked.Exchange(ref this.messageQueue, new ConcurrentQueue<Message>());
            this.receivingSemaphore.Dispose();
            Interlocked.Exchange(ref this.receivingSemaphore, new SemaphoreSlim(0));
            if (TryStateTransition(StateFlags.TransientError, StateFlags.NotInitialized))
            {
                this.resetConnectionCompletion.TryComplete();
                Interlocked.Exchange(ref this.resetConnectionCompletion, new TaskCompletionSource());
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
            IEventLoopGroup eventLoopGroup = EventLoopGroupPool.TakeOrAdd(eventLoopGroupKey,
                () => new MultithreadEventLoopGroup(() => new SingleThreadEventLoop("MQTTExecutionThread", TimeSpan.FromSeconds(1)), 1));

            Bootstrap bootstrap = new Bootstrap()
                .Group(eventLoopGroup)
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

            ScheduleCleanup(() =>
            {
                EventLoopGroupPool.Release(this.eventLoopGroupKey);
                return TaskConstants.Completed;
            });

            return bootstrap.ConnectAsync;
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
            foreach (StateFlags state in new[] { StateFlags.NotInitialized, StateFlags.Opening, StateFlags.Open, StateFlags.Subscribing, StateFlags.Receiving, StateFlags.TransientError, StateFlags.Closing, StateFlags.Closed, StateFlags.Disposed, StateFlags.Fatal })
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

        public bool ShouldRetry(out int timeoutInMs)
        {
            if (this.currentRetryCount < this.maxRetryCount)
            {
                var random = new Random();
                int num = (int)Math.Min(
                    this.minBackoff.TotalMilliseconds + (Math.Pow(2, currentRetryCount) - 1) * random.Next((int)(this.deltaBackoff.TotalMilliseconds * 0.8), (int)(this.deltaBackoff.TotalMilliseconds * 1.2)), 
                    this.maxBackoff.TotalMilliseconds);
                this.currentRetryCount++;
                timeoutInMs = num;
                return true;
            }
            timeoutInMs = -1;
            return false;

        }

        async Task EnsureWorkingStatesAsync(bool reopen)
        {
            StateFlags state = this.State;

            if ((state & (StateFlags.NotInitialized | StateFlags.Opening)) > 0)
            {
                await OnOpenAsync(true);
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

            if (state == StateFlags.TransientError)
            {
                if (!reopen)
                {
                    throw new IotHubClientException(this.transientException);
                }

                Task<int> currentResetCompletion = this.resetConnectionCompletion.Task;
                if (TryStateTransition(StateFlags.TransientError, StateFlags.TransientError))
                {
                    await currentResetCompletion;
                }

                if (!await this.TryOpenAsync())
                {
                    await HandleInvalidState(this.State, false);
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