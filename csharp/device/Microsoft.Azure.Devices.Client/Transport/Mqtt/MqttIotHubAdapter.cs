// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using DotNetty.Buffers;
    using DotNetty.Codecs.Mqtt.Packets;
    using DotNetty.Common.Concurrency;
    using DotNetty.Common.Utilities;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Channels;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    sealed class MqttIotHubAdapter : ChannelHandlerAdapter
    {
        const string TelemetryTopicFilterFormat = "devices/{0}/messages/devicebound/+/";

        static readonly Action<object> PingServerCallback = PingServer;
        static readonly Action<object> CheckConackTimeoutCallback = CheckConnectionTimeout;
        static readonly Action<Task, object> ShutdownOnWriteFaultAction = (task, ctx) => ShutdownOnError((IChannelHandlerContext)ctx, "WriteAndFlushAsync", task.Exception);
        static readonly ThreadLocal<Random> ThreadLocalRandom = new ThreadLocal<Random>(() => new Random((int)DateTime.UtcNow.ToFileTimeUtc()));

        StateFlags stateFlags;

        readonly Settings settings;
        readonly ITopicNameRouter topicNameRouter;
        readonly IWillMessageProvider willMessageProvider;
        readonly ISessionStatePersistenceProvider sessionStatePersistenceProvider;
        readonly Action connectCallback;
        readonly Action disconnectCallback;
        readonly Action<Message> onMessageReceived;
        ISessionState sessionState;
        
        readonly string deviceId;
        readonly string iotHubHostName;
        readonly string password;
        readonly TimeSpan? keepAliveTimeout;
        readonly TimeSpan pingRequestTimeout;
        readonly QualityOfService maxSupportedQos;
        QualityOfService maxQosToPublish;
        DateTime lastClientActivityTime;

        readonly SimpleWorkQueue<PublishWorkItem> serviceBoundPublishProcessor;
        readonly SimpleWorkQueue<PublishWorkItem> serviceBoundPubAckProcessor;
        
        readonly SimpleWorkQueue<PublishPacket> deviceBoundPublishProcessor;
        readonly SimpleWorkQueue<PublishPacket> deviceBoundPubAckProcessor;
        
        Queue<Packet> connectWithSubscribeQueue;
        Queue<Packet> ConnectPendingQueue { get { return this.connectWithSubscribeQueue ?? (this.connectWithSubscribeQueue = new Queue<Packet>(4)); } }
        int InboundBacklogSize { get { return this.serviceBoundPublishProcessor.BacklogSize + this.serviceBoundPubAckProcessor.BacklogSize; } }
        
        public MqttIotHubAdapter(
            string deviceId, 
            string iotHubHostName, 
            string password, 
            Settings settings, 
            ISessionStatePersistenceProvider sessionStatePersistenceProvider, 
            ITopicNameRouter topicNameRouter, 
            IWillMessageProvider willMessageProvider, 
            Action connectCallback, 
            Action disconnectCallback, 
            Action<Message> onMessageReceived)
        {

            Contract.Requires(deviceId != null);
            Contract.Requires(iotHubHostName != null);
            Contract.Requires(password != null);
            Contract.Requires(settings != null);
            Contract.Requires(sessionStatePersistenceProvider != null);
            Contract.Requires(topicNameRouter != null);
            if (settings.HasWill)
            {
                Contract.Requires(willMessageProvider != null);
            }

            this.deviceId = deviceId;
            this.iotHubHostName = iotHubHostName;
            this.password = password;
            this.settings = settings;
            this.sessionStatePersistenceProvider = sessionStatePersistenceProvider;
            this.topicNameRouter = topicNameRouter;
            this.willMessageProvider = willMessageProvider;
            this.connectCallback = connectCallback;
            this.disconnectCallback = disconnectCallback;
            this.onMessageReceived = onMessageReceived;
            this.maxSupportedQos = QualityOfService.AtLeastOnce;
            this.keepAliveTimeout = this.settings.KeepAliveInSeconds > 0 ? TimeSpan.FromSeconds(this.settings.KeepAliveInSeconds) : (TimeSpan?)null;
            this.pingRequestTimeout = this.settings.KeepAliveInSeconds > 0 ? TimeSpan.FromSeconds(this.settings.KeepAliveInSeconds / 2d) : TimeSpan.MaxValue;
        }

        #region IChannelHandler overrides

        public override void ChannelActive(IChannelHandlerContext context)
        {
            this.stateFlags = StateFlags.NotConnected;

            context.Channel.EventLoop.ScheduleAsync(this.ConnectCallback, context, TimeSpan.MaxValue);

            base.ChannelActive(context);
        }

        public override Task WriteAsync(IChannelHandlerContext context, object data)
        {
            string lockToken = data as string;
            if (lockToken != null)
            {
                int packetId;
                if (int.TryParse(lockToken, out packetId))
                {
                    this.deviceBoundPubAckProcessor.Acknowledge(packetId);
                }
            }
            
            var message = data as Message;
            if (message != null)
            {
                var publishPacket = new PublishPacket(this.settings.PublishToServerQoS, false, false);
                var publishCompletion = new TaskCompletionSource();
                var workItem = new PublishWorkItem
                {
                    Completion = publishCompletion,
                    Packet = publishPacket
                };
                switch (this.settings.PublishToServerQoS)
                {
                    case QualityOfService.AtMostOnce:
                        this.serviceBoundPublishProcessor.Post(context, workItem);
                        break;
                    case QualityOfService.AtLeastOnce:
                        this.serviceBoundPubAckProcessor.Post(context, workItem);
                        break;
                    default:
                        throw new NotSupportedException(string.Format("Unsuppoerted telemetry QoS: '{0}'", this.settings.PublishToServerQoS));
                }
                return publishCompletion.Task;
            }
            throw new InvalidOperationException(string.Format("Unexpected data type: '{0}'", data.GetType().Name));
        }
        
        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            var packet = message as Packet;
            if (packet == null)
            {
                return;
            }

            this.lastClientActivityTime = DateTime.UtcNow; // notice last client activity - used in handling disconnects on keep-alive timeout

            if (this.IsInState(StateFlags.Connected) || packet.PacketType == PacketType.CONNECT)
            {
                this.ProcessMessage(context, packet);
            }
            else
            {
                if (this.IsInState(StateFlags.Connecting))
                {
                    this.ConnectPendingQueue.Enqueue(packet);
                }
                else
                {
                    // we did not start processing CONNECT yet which means we haven't received it yet but the packet of different type has arrived.
                    ShutdownOnError(context);
                }
            }
        }

        public override void ChannelReadComplete(IChannelHandlerContext context)
        {
            base.ChannelReadComplete(context);
            int inboundBacklogSize = this.InboundBacklogSize;
            if (inboundBacklogSize < this.settings.MaxPendingInboundMessages)
            {
                context.Read();
            }
        }

        public override void ChannelInactive(IChannelHandlerContext context)
        {
            this.Shutdown(context);

            base.ChannelInactive(context);
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            ShutdownOnError(context);
        }

        public override void UserEventTriggered(IChannelHandlerContext context, object @event)
        {
            var handshakeCompletionEvent = @event as TlsHandshakeCompletionEvent;
            if (handshakeCompletionEvent != null && !handshakeCompletionEvent.IsSuccessful)
            {
                ShutdownOnError(context, "TLS", handshakeCompletionEvent.Exception);
            }
        }

        #endregion

        //START

        void ConnectCallback(object state)
        {
            var context = (IChannelHandlerContext)state;
            var connectPacket = new ConnectPacket
            {
                ClientId = this.deviceId,
                HasUsername = true,
                Username = this.iotHubHostName + "/" + this.deviceId,
                HasPassword = true,
                Password = this.password,
                KeepAliveInSeconds = this.settings.KeepAliveInSeconds,
                CleanSession = this.settings.CleanSession,
                HasWill = this.settings.HasWill
            };
            if (connectPacket.HasWill)
            {
                connectPacket.WillMessage = this.GetWillMessageBody(this.willMessageProvider.Message);
                connectPacket.WillQualityOfService = this.willMessageProvider.QoS;
                connectPacket.WillRetain = false;
                string willtopicName;
                if (!this.topicNameRouter.TryMapRouteToTopicName(RouteSourceType.Notification, this.willMessageProvider.Message.Properties, out willtopicName))
                {
                    this.Shutdown(context);
                    context.FireExceptionCaught(new IotHubClientException("Cannot create will topic. Please check topic name router settings and will message properties."));
                }
                connectPacket.WillTopicName = willtopicName;
            }
            this.stateFlags = StateFlags.Connecting;

            //Used to send PINGREQ in case of long time of inactivity

            Util.WriteMessageAsync(context, connectPacket).OnFault(ShutdownOnWriteFaultAction, context);

            if (this.keepAliveTimeout.HasValue)
            {
                this.lastClientActivityTime = DateTime.UtcNow;
                context.Channel.EventLoop.ScheduleAsync(PingServerCallback, context, this.pingRequestTimeout);
            }

            TimeSpan? timeout = this.settings.ConnectArrivalTimeout;
            if (timeout.HasValue)
            {
                context.Channel.EventLoop.ScheduleAsync(CheckConackTimeoutCallback, context, timeout.Value);
            }

            string topicFilter = TelemetryTopicFilterFormat.FormatInvariant(this.deviceId);
            var subscribePacket = new SubscribePacket(GenerateNextPacketId(), new SubscriptionRequest(topicFilter, this.settings.PublishToServerQoS));

            Util.WriteMessageAsync(context, subscribePacket).OnFault(ShutdownOnWriteFaultAction, context);
            this.ResumeReadingIfNecessary(context);
        }

        void CompleteConnect(IChannelHandlerContext context)
        {
            if (this.keepAliveTimeout > TimeSpan.Zero)
            {
                PingServer(context);
            }

            this.stateFlags = StateFlags.Connected;
        }

        static void CheckConnectionTimeout(object state)
        {
            var context = (IChannelHandlerContext)state;
            var handler = (MqttIotHubAdapter)context.Handler;
            if (handler.IsInState(StateFlags.Connecting))
            {
                ShutdownOnError(context);
            }
        }

        static void PingServer(object ctx)
        {
            var context = (IChannelHandlerContext)ctx;
            var self = (MqttIotHubAdapter)context.Handler;
            TimeSpan remainingTime = DateTime.UtcNow - self.lastClientActivityTime - self.pingRequestTimeout;
            if (remainingTime.TotalSeconds > 0)
            {
                Util.WriteMessageAsync(context, PingReqPacket.Instance).OnFault(f => context.FireExceptionCaught(f.Exception));
            }

            context.Channel.EventLoop.ScheduleAsync(PingServerCallback, context, self.pingRequestTimeout);
        }

        void ProcessMessage(IChannelHandlerContext context, Packet packet)
        {
            if (this.IsInState(StateFlags.Closed))
            {
                return;
            }

            switch (packet.PacketType)
            {
                case PacketType.CONNACK:
                    this.ProcessConnect(context, (ConnAckPacket)packet);
                    break;
                case PacketType.PUBLISH:
                    this.ProcessPublish(context, (PublishPacket)packet);
                    break;
                case PacketType.PUBACK:
                    this.serviceBoundPubAckProcessor.Acknowledge(((PubAckPacket)packet).PacketId);
                    break;
                case PacketType.SUBACK:
                    this.ProcessSubscribe(context, (SubAckPacket)packet);
                    break;
                case PacketType.UNSUBACK:
                    this.ResumeReadingIfNecessary(context);
                    break;
                default:
                    ShutdownOnError(context);
                    break;
            }
        }

        async void ProcessConnect(IChannelHandlerContext context, ConnAckPacket packet)
        {
            Exception exception = null;
            try
            {
                if (packet.ReturnCode != ConnectReturnCode.Accepted)
                {
                    string reason = "CONNECT failed: " + packet.ReturnCode;
                    ShutdownOnError(context);
                    throw new IotHubException(reason);
                }

                if (!this.IsInState(StateFlags.Connecting))
                {
                    string reason = "CONNECT has been received in current session already. Only one CONNECT is expected per session.";
                    ShutdownOnError(context);
                    throw new IotHubException(reason);
                }

                this.stateFlags = StateFlags.Connected;

                await this.EstablishSessionStateAsync(this.deviceId, this.settings.CleanSession, packet.SessionPresent);

                this.CompleteConnect(context);

                this.StartReceiving(context);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            if (exception != null)
            {
                ShutdownOnError(context, "CONNECT", exception);

                throw exception;
            }
        }

        void ProcessSubscribe(IChannelHandlerContext context, SubAckPacket packet)
        {
            this.maxQosToPublish = packet.QualityOfService;
            this.ResumeReadingIfNecessary(context);
        }

        void ProcessPublish(IChannelHandlerContext context, PublishPacket packet)
        {
            switch (packet.QualityOfService)
            {
                case QualityOfService.AtMostOnce:
                    this.deviceBoundPublishProcessor.Post(context, packet);
                    break;
                case QualityOfService.AtLeastOnce:
                    this.deviceBoundPubAckProcessor.Post(context, packet);
                    break;
                default:
                    throw new NotSupportedException(string.Format("Unexpected QoS: '{0}'", packet.QualityOfService));
            }
        }

        //STOP
        
        void ShutdownOnReceiveError(IChannelHandlerContext context, string exception)
        {
            this.serviceBoundPubAckProcessor.Abort();
            this.serviceBoundPublishProcessor.Abort();

            ShutdownOnError(context, "Receive", exception);
        }

        static void ShutdownOnError(IChannelHandlerContext context, string scope, Exception exception)
        {
            ShutdownOnError(context, scope, exception.ToString());
        }

        static void ShutdownOnError(IChannelHandlerContext context, string scope, string exception)
        {
            ShutdownOnError(context);
        }

        /// <summary>
        ///     Logs error and initiates closure of both channel and hub connection.
        /// </summary>
        /// <param name="context"></param>
        static void ShutdownOnError(IChannelHandlerContext context)
        {
            var self = (MqttIotHubAdapter)context.Handler;
            if (!self.IsInState(StateFlags.Closed))
            {
                self.Shutdown(context);
            }
        }

        /// <summary>
        ///     Closes channel
        /// </summary>
        /// <param name="context"></param>
        async void Shutdown(IChannelHandlerContext context)
        {
            if (this.IsInState(StateFlags.Closed))
            {
                return;
            }

            try
            {
                this.stateFlags |= StateFlags.Closed; // "or" not to interfere with ongoing logic which has to honor Closed state when it's right time to do (case by case)

                Queue<Packet> connectQueue = this.connectWithSubscribeQueue;
                if (connectQueue != null)
                {
                    while (connectQueue.Count > 0)
                    {
                        Packet packet = connectQueue.Dequeue();
                        ReferenceCountUtil.Release(packet);
                    }
                }

                this.CloseIotHubConnection();
                await context.CloseAsync();
            }
            catch (Exception ex)
            {
            }
        }

        async void CloseIotHubConnection()
        {
            if (this.IsInState(StateFlags.NotConnected))
            {
                // closure happened before IoT Hub connection was established or it was initiated due to disconnect
                return;
            }

            try
            {
                this.serviceBoundPublishProcessor.Complete();
                this.serviceBoundPubAckProcessor.Complete();
                await Task.WhenAll(this.serviceBoundPublishProcessor.Completion, this.serviceBoundPubAckProcessor.Completion);
            }
            catch
            {
                // ignored on closing
            }
        }

        #region helper methods

        void ResumeReadingIfNecessary(IChannelHandlerContext context)
        {
            if (this.InboundBacklogSize == this.settings.MaxPendingInboundMessages - 1) // we picked up a packet from full queue - now we have more room so order another read
            {
                context.Read();
            }
        }

        void StartReceiving(IChannelHandlerContext context)
        {
            this.stateFlags |= StateFlags.Receiving;
            try
            {
                //TODO:
            }
            catch (IotHubException ex)
            {
                this.ShutdownOnReceiveError(context, ex.ToString());
            }
            catch (Exception ex)
            {
                ShutdownOnError(context, "Receive", ex.ToString());
            }
        }

        bool IsInState(StateFlags stateFlagsToCheck)
        {
            return (this.stateFlags & stateFlagsToCheck) == stateFlagsToCheck;
        }

        IByteBuffer GetWillMessageBody(Message message)
        {
            Stream bodyStream = message.GetBodyStream();
            var buffer = new byte[bodyStream.Length];
            bodyStream.Read(buffer, 0, buffer.Length);
            IByteBuffer copiedBuffer = Unpooled.CopiedBuffer(buffer);
            return copiedBuffer;
        }

        /// <summary>
        ///     Loads and updates (as necessary) session state.
        /// </summary>
        /// <param name="clientId">Client identificator to load the session state for.</param>
        /// <param name="cleanSession">Determines whether session has to be deleted if it already exists.</param>
        /// <param name="sessionPresent">Determines wether session present on server</param>
        /// <returns></returns>
        async Task EstablishSessionStateAsync(string clientId, bool cleanSession, bool sessionPresent)
        {
            ISessionState existingSessionState = await this.sessionStatePersistenceProvider.GetAsync(clientId);

            if (existingSessionState != null)
            {
                if (!sessionPresent || cleanSession)
                {
                    await this.sessionStatePersistenceProvider.DeleteAsync(clientId, existingSessionState);
                    this.sessionState = this.sessionStatePersistenceProvider.Create(true);
                }
                else
                {
                    this.sessionState = existingSessionState;
                }
            }
            else
            {
                this.sessionState = this.sessionStatePersistenceProvider.Create(true);
            }
        }
        /// <summary>
        ///     Performs complete initialization of <see cref="MqttIotHubAdapter" /> based on received CONNECT packet.
        /// </summary>
        /// <param name="context"><see cref="IChannelHandlerContext" /> instance.</param>
        /// <param name="packet">CONNECT packet.</param>
        static int GenerateNextPacketId()
        {
            return ThreadLocalRandom.Value.Next(1, ushort.MaxValue);
        }
        #endregion


        public enum RouteSourceType
        {
            Unknown,
            Notification
        }

        [Flags]
        enum StateFlags
        {
            NotConnected = 1,
            Connecting = 1 << 1,
            Connected = 1 << 2,
            Receiving = 1 << 3,
            Closed = 1 << 4
        }

        class PublishWorkItem
        {
            public PublishPacket Packet { get; set; }
            public TaskCompletionSource Completion { get; set; }
        }
    }
}