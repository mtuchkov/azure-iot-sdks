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
            Disposing
        }

        static readonly ConcurrentObjectPool<string, IEventLoopGroup> EventLoopGroupPool =
            new ConcurrentObjectPool<string, IEventLoopGroup>(Environment.ProcessorCount, TimeSpan.FromSeconds(5), elg => elg.ShutdownGracefullyAsync());
        
        static readonly TimeSpan DefaultReceiveTimeoutInSeconds = TimeSpan.FromMinutes(1);

        readonly Func<IPAddress, int, Task<IChannel>> channelFactory;
        readonly string eventLoopGroupKey;
        readonly IPAddress serverAddress;
        readonly ConcurrentQueue<Message> messageQueue;
        readonly ConcurrentQueue<string> completionQueue;
        readonly MqttIotHubAdapterFactory mqttIotHubAdapterFactory;
        readonly QualityOfService qos;
        readonly object syncRoot = new object();
        readonly SemaphoreSlim receivingSemaphore = new SemaphoreSlim(0);
        readonly CancellationTokenSource disconnectAwaitersCancellationSource = new CancellationTokenSource();
        TaskCompletionSource connectCompletion = new TaskCompletionSource();
        TaskCompletionSource reestablishConnectionCompletion = new TaskCompletionSource();
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
                    this.connectCompletion.TrySetException(ex);
                }
                this.ScheduleCleanup(async () =>
                {
                    this.disconnectAwaitersCancellationSource.Cancel();
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

                this.TryStateTransition(StateFlags.Opening, StateFlags.Open);
            }
            else
            {
                await this.connectCompletion.Task;
            }
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
            try
            {
                await this.EnsureStateAsync(StateFlags.Open | StateFlags.Subscribing | StateFlags.Receiving);

                await this.channel.WriteAndFlushAsync(message);
            }
            catch (NullReferenceException ex)
            {
                if (this.channel == null)
                {
                    throw new NullReferenceException("Channel is null",ex);
                }
                throw new InvalidOperationException("Unhandled null ref", ex);
            }
        }

        protected override async Task OnSendEventAsync(IEnumerable<Message> messages)
        {
            await this.EnsureStateAsync(StateFlags.Open | StateFlags.Subscribing | StateFlags.Receiving);

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
                    this.reestablishConnectionCompletion.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                }
                else if (previousState == StateFlags.Open || previousState == StateFlags.Subscribing)
                {
                    this.subscribeCompletionSource.TrySetCanceled();
                    this.reestablishConnectionCompletion.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                }
                else if (previousState == StateFlags.Receiving)
                {
                    this.reestablishConnectionCompletion.TrySetCanceled();
                    this.disconnectAwaitersCancellationSource.Cancel();
                }
                else if (previousState == StateFlags.TransientError)
                {
                    this.connectCompletion.TrySetCanceled();
                    this.subscribeCompletionSource.TrySetCanceled();
                    this.reestablishConnectionCompletion.TrySetCanceled();
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
            await this.EnsureStateAsync(StateFlags.Open | StateFlags.Subscribing | StateFlags.Receiving);

            await this.SubscribeAsync();

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
            await this.EnsureStateAsync(StateFlags.Receiving);
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
            this.connectCompletion.TryComplete();
        }

        void OnMessageReceived(Message message)
        {
            this.messageQueue.Enqueue(message);
            this.receivingSemaphore.Release();
        }

        async void OnError(Exception exception)
        {
            var initialCompletionSources = new[] { this.connectCompletion, this.subscribeCompletionSource, this.reestablishConnectionCompletion };
            try
            {
                if (this.TryStateTransition(StateFlags.Opening | StateFlags.Open, StateFlags.TransientError))
                {
                    if (await this.ReopenAsync())
                    {
                        this.reestablishConnectionCompletion.TryComplete();
                    }
                    else
                    {
                        this.Abort(initialCompletionSources, exception);
                    }
                }
                if (this.TryStateTransition(StateFlags.Subscribing | StateFlags.Receiving, StateFlags.TransientError))
                {
                    await this.ReopenAsync();
                    await this.SubscribeAsync();
                    this.reestablishConnectionCompletion.TryComplete();
                }
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                this.Abort(initialCompletionSources, new AggregateException(exception, ex));
            }
        }

        #endregion

        async Task StopAsync()
        {
            try
            {
                await this.closeRetryPolicy.ExecuteAsync(() => this.cleanupFunc());
                this.connectCompletion.TrySetCanceled();
                this.subscribeCompletionSource.TrySetCanceled();
                this.reestablishConnectionCompletion.TrySetCanceled();
                this.TryStateTransition(StateFlags.Closing, StateFlags.Closed);
            }
            catch (Exception ex)
            {
                this.Abort(new[] { this.connectCompletion, this.subscribeCompletionSource, this.reestablishConnectionCompletion }, ex);
                this.State = StateFlags.Fatal;
                throw;
            }
        }

        async Task SubscribeAsync()
        {
            if (this.TryStateTransition(StateFlags.Open, StateFlags.Subscribing))
            {
                try
                {
                    await this.channel.WriteAsync(new SubscribePacket());
                    this.subscribeCompletionSource.TryComplete();
                    this.TryStateTransition(StateFlags.Subscribing, StateFlags.Receiving);
                }
                catch (Exception ex)
                {
                    this.subscribeCompletionSource.TrySetException(ex);
                }
            }
            else
            {
                await this.subscribeCompletionSource.Task;
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
            this.reestablishConnectionCompletion?.TrySetException(exception);
            this.fatalException = exception;
            this.State = StateFlags.Fatal;
        }

        async Task<bool> ReopenAsync()
        {
            int timeoutInMs;
            if (this.ShouldRetry(out timeoutInMs))
            {
                await Task.Delay(timeoutInMs);

                await this.closeRetryPolicy.ExecuteAsync(() => this.cleanupFunc());

                this.State = StateFlags.NotInitialized;
                await this.OnOpenAsync(true);

                return true;
            }
            return false;
        }

        StateFlags AtomicStatusChange(StateFlags newState)
        {
            StateFlags oldState = this.State;
            SpinWait.SpinUntil(() => (oldState = (StateFlags)Interlocked.CompareExchange(ref this.transportStatus, (long)newState, (long)oldState)) != newState);
            return oldState;
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
                await this.closeRetryPolicy.ExecuteAsync(() => this.cleanupFunc());
            }
            catch (Exception ex) when (!ex.IsFatal())
            {

            }
        }

        bool TryStateTransition(StateFlags fromState, StateFlags toState)
        {
            foreach (StateFlags state in new[] { StateFlags.NotInitialized, StateFlags.Opening, StateFlags.Open, StateFlags.Subscribing, StateFlags.Receiving, StateFlags.TransientError, StateFlags.Disposing, StateFlags.Closing, StateFlags.Closed, StateFlags.Disposed, StateFlags.Fatal })
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

        async Task EnsureStateAsync(StateFlags states)
        {
            StateFlags state = this.State;

            if ((state & states) > 0)
            {
                return;
            }

            if (state == StateFlags.Disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }

            if (state == StateFlags.TransientError)
            {
                await this.reestablishConnectionCompletion.Task;
            }

            if (state == StateFlags.Fatal)
            {
                throw new IotHubClientException("The client is in a bad state.", this.fatalException);
            }

            throw new IotHubCommunicationException("Connection is closed.");
        }
    }
}