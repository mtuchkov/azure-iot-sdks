// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DotNetty.Buffers;
    using DotNetty.Codecs.Mqtt.Packets;
    using DotNetty.Common;
    using DotNetty.Common.Utilities;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Channels;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    sealed class MqttIotHubAdapter : ChannelHandlerAdapter, IMqttIotHubAdapter
    {
        const string UnmatchedFlagPropertyName = "Unmatched";
        const string SubjectPropertyName = "Subject";
        const string DeviceIdParam = "deviceId";

        static readonly Action<object> PingServerCallback = PingServer;
        static readonly Action<object> CheckConackTimeoutCallback = CheckConnectionTimeout;
        static readonly Action<Task, object> ShutdownOnWriteFaultAction = (task, ctx) => ShutdownOnError((IChannelHandlerContext)ctx, "WriteAndFlushAsync", task.Exception);
        static readonly Action<Task, object> ShutdownOnPublishFaultAction = (task, ctx) => ShutdownOnError((IChannelHandlerContext)ctx, "<- PUBLISH", task.Exception);
        static readonly Action<Task> ShutdownOnPublishToServerFaultAction = CreateScopedFaultAction("-> PUBLISH");
        static readonly Action<Task> ShutdownOnPubAckFaultAction = CreateScopedFaultAction("-> PUBACK");

        readonly Settings settings;
        readonly ITopicNameRouter topicNameRouter;
        readonly IWillMessageProvider willMessageProvider;
        Dictionary<string, string> sessionContext;
        ISessionState sessionState;
        
        readonly string deviceId;
        readonly string iotHubHostName;
        readonly string password;
        readonly TimeSpan? keepAliveTimeout;
        readonly TimeSpan pingRequestTimeout;
        readonly QualityOfService maxSupportedQos;
        DateTime lastClientActivityTime;
        
        readonly PacketAsyncProcessor<PublishPacket> publishProcessor;
        readonly RequestAckPairProcessor<AckPendingMessageState, PublishPacket> publishPubAckProcessor;
        readonly ISessionStatePersistenceProvider sessionStateManager;
        Queue<Packet> connectWithSubscribeQueue;
        Queue<Packet> subscriptionChangeQueue;
        
        StateFlags stateFlags;

        Queue<Packet> SubscriptionChangeQueue { get { return this.subscriptionChangeQueue ?? (this.subscriptionChangeQueue = new Queue<Packet>(4)); } }
        Queue<Packet> ConnectPendingQueue { get { return this.connectWithSubscribeQueue ?? (this.connectWithSubscribeQueue = new Queue<Packet>(4)); } }
        int InboundBacklogSize { get { return this.publishProcessor.BacklogSize + this.publishPubAckProcessor.BacklogSize; } }
        int MessagePendingAckCount { get { return this.publishPubAckProcessor.RequestPendingAckCount; } }

        public MqttIotHubAdapter(
            string deviceId,
            string iotHubHostName,
            string password,
            Settings settings, 
            ISessionStatePersistenceProvider sessionStateManager,
            ITopicNameRouter topicNameRouter,
            IWillMessageProvider willMessageProvider)
        {

            Contract.Requires(deviceId != null);
            Contract.Requires(iotHubHostName != null);
            Contract.Requires(password != null);
            Contract.Requires(settings != null);
            Contract.Requires(sessionStateManager != null);
            Contract.Requires(topicNameRouter != null);
            if (settings.HasWill)
            {
                Contract.Requires(willMessageProvider != null);
            }

            this.deviceId = deviceId;
            this.iotHubHostName = iotHubHostName;
            this.password = password;
            this.settings = settings;
            this.sessionStateManager = sessionStateManager;
            this.topicNameRouter = topicNameRouter;
            this.willMessageProvider = willMessageProvider;
            this.maxSupportedQos = QualityOfService.AtLeastOnce;
            this.keepAliveTimeout = this.settings.KeepAliveInSeconds > 0 ? TimeSpan.FromSeconds(this.settings.KeepAliveInSeconds) : (TimeSpan?)null;
            this.pingRequestTimeout = this.settings.KeepAliveInSeconds > 0 ? TimeSpan.FromSeconds(this.settings.KeepAliveInSeconds / 2d) : TimeSpan.MaxValue;

            this.publishProcessor = new PacketAsyncProcessor<PublishPacket>(this.PublishToServerAsync);
            this.publishProcessor.Completion.OnFault(ShutdownOnPublishToServerFaultAction);
            
            TimeSpan? ackTimeout = this.settings.DeviceReceiveAckCanTimeout ? this.settings.DeviceReceiveAckTimeout : null;
            this.publishPubAckProcessor = new RequestAckPairProcessor<AckPendingMessageState, PublishPacket>(this.AcknowledgePublishAsync, this.RetransmitNextPublish, ackTimeout);
            this.publishPubAckProcessor.Completion.OnFault(ShutdownOnPubAckFaultAction);
        }

        #region IChannelHandler overrides

        public override void ChannelActive(IChannelHandlerContext context)
        {
            this.stateFlags = StateFlags.NotConnected;

            context.Channel.EventLoop.ScheduleAsync(this.ConnectCallback, context, TimeSpan.MaxValue);

            base.ChannelActive(context);
        }

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
                connectPacket.WillMessage = Unpooled.CopiedBuffer(Encoding.UTF8.GetBytes(this.willMessageProvider.Message));
                connectPacket.WillQualityOfService = this.willMessageProvider.QoS;
                connectPacket.WillRetain = false;
                string willtopicName;
                if (!this.topicNameRouter.TryMapRouteToTopicName(RouteSourceType.Notification, this.willMessageProvider.Properties, out willtopicName))
                {
                    this.Shutdown(context, true);
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

            context.Read();
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
                    ShutdownOnError(context, string.Format("First packet in the session must be CONNECT. Observed: {0}", packet));
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
            this.Shutdown(context, false);

            base.ChannelInactive(context);
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            ShutdownOnError(context, "Exception encountered: " + exception);
        }

        public override void UserEventTriggered(IChannelHandlerContext context, object @event)
        {
            var handshakeCompletionEvent = @event as TlsHandshakeCompletionEvent;
        }

        #endregion

        void ProcessMessage(IChannelHandlerContext context, Packet packet)
        {
            if (this.IsInState(StateFlags.Closed))
            {
                return;
            }

            switch (packet.PacketType)
            {
                case PacketType.CONNACK:
                    this.AcknowledgeConnection(context, (ConnAckPacket)packet);
                    break;
                case PacketType.PUBLISH:
                    this.publishProcessor.Post(context, (PublishPacket)packet);
                    break;
                case PacketType.PUBACK:
                    this.publishPubAckProcessor.Post(context, (PubAckPacket)packet);
                    break;
                case PacketType.SUBSCRIBE:
                case PacketType.UNSUBSCRIBE:
                    this.HandleSubscriptionChange(context, packet);
                    break;
                case PacketType.PINGREQ:
                    // no further action is needed - keep-alive "timer" was reset by now
                    Util.WriteMessageAsync(context, PingRespPacket.Instance)
                        .OnFault(ShutdownOnWriteFaultAction, context);
                    break;
                case PacketType.DISCONNECT:
                    this.Shutdown(context, true);
                    break;
                default:
                    ShutdownOnError(context, string.Format("Packet of unsupported type was observed: {0}", packet));
                    break;
            }
        }

        #region SUBSCRIBE / UNSUBSCRIBE handling

        void HandleSubscriptionChange(IChannelHandlerContext context, Packet packet)
        {
            this.SubscriptionChangeQueue.Enqueue(packet);

            if (!this.IsInState(StateFlags.ChangingSubscriptions))
            {
                this.stateFlags |= StateFlags.ChangingSubscriptions;
                this.ProcessPendingSubscriptionChanges(context);
            }
        }

        async void ProcessPendingSubscriptionChanges(IChannelHandlerContext context)
        {
            try
            {
                do
                {
                    ISessionState newState = this.sessionState.Copy();
                    Queue<Packet> queue = this.SubscriptionChangeQueue;
                    var acks = new List<Packet>(queue.Count);
                    foreach (Packet packet in queue) // todo: if can queue be null here, don't force creation
                    {
                        switch (packet.PacketType)
                        {
                            case PacketType.SUBSCRIBE:
                                acks.Add(Util.AddSubscriptions(newState, (SubscribePacket)packet, this.maxSupportedQos));
                                break;
                            case PacketType.UNSUBSCRIBE:
                                acks.Add(Util.RemoveSubscriptions(newState, (UnsubscribePacket)packet));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                    queue.Clear();

                    if (!this.sessionState.IsTransient)
                    {
                        // save updated session state, make it current once successfully set
                        await this.sessionStateManager.SetAsync(this.deviceId, newState);
                    }

                    this.sessionState = newState;

                    // release ACKs

                    var tasks = new List<Task>(acks.Count);
                    foreach (Packet ack in acks)
                    {
                        tasks.Add(context.WriteAsync(ack));
                    }
                    context.Flush();
                    await Task.WhenAll(tasks);
                }
                while (this.subscriptionChangeQueue.Count > 0);

                this.ResetState(StateFlags.ChangingSubscriptions);
            }
            catch (Exception ex)
            {
                ShutdownOnError(context, "-> UN/SUBSCRIBE", ex);
            }
        }

        #endregion

        #region PUBLISH Client -> Server handling

        async Task PublishToServerAsync(IChannelHandlerContext context, PublishPacket packet)
        {
            if (!this.IsInState(StateFlags.Closed))
            {
                return;
            }

            PreciseTimeSpan startedTimestamp = PreciseTimeSpan.FromStart;

            this.ResumeReadingIfNecessary(context);

            using (Stream bodyStream = packet.Payload.IsReadable() ? new ReadOnlyByteBufferStream(packet.Payload, true) : null)
            {
                var message = new Message(bodyStream);
                this.ApplyRoutingConfiguration(message, packet);

                Util.CompleteMessageFromPacket(message, packet, this.settings);

                await context.WriteAsync(packet);
            }

            if (!this.IsInState(StateFlags.Closed))
            {
                switch (packet.QualityOfService)
                {
                    case QualityOfService.AtMostOnce:
                        // no response necessary
                        break;
                    case QualityOfService.AtLeastOnce:
                        Util.WriteMessageAsync(context, PubAckPacket.InResponseTo(packet))
                            .OnFault(ShutdownOnWriteFaultAction, context);
                        break;
                    case QualityOfService.ExactlyOnce:
                        ShutdownOnError(context, "QoS 2 is not supported.");
                        break;
                    default:
                        throw new InvalidOperationException("Unexpected QoS level: " + packet.QualityOfService);
                }
            }
        }

        void ResumeReadingIfNecessary(IChannelHandlerContext context)
        {
            if (this.InboundBacklogSize == this.settings.MaxPendingInboundMessages - 1) // we picked up a packet from full queue - now we have more room so order another read
            {
                context.Read();
            }
        }

        void ApplyRoutingConfiguration(Message message, PublishPacket packet)
        {
            RouteDestinationType routeType;
            if (this.topicNameRouter.TryMapTopicNameToRoute(packet.TopicName, out routeType, message.Properties))
            {
                // successfully matched topic against configured routes -> validate topic name
                string messageDeviceId;
                if (message.Properties.TryGetValue(DeviceIdParam, out messageDeviceId))
                {
                    message.Properties.Remove(DeviceIdParam);
                }
            }
            else
            {
                routeType = RouteDestinationType.Telemetry;
                message.Properties[UnmatchedFlagPropertyName] = bool.TrueString;
                message.Properties[SubjectPropertyName] = packet.TopicName;
            }

            // once we have different routes, this will change to tackle different aspects of route types
            switch (routeType)
            {
                case RouteDestinationType.Telemetry:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(string.Format("Unexpected route type: {0}", routeType));
            }
        }

        #endregion

        #region PUBLISH Server -> Client handling

        async void Receive(IChannelHandlerContext context)
        {
            Contract.Requires(this.sessionContext != null);

            try
            {
                Message message = null;//await this.iotHubClient.ReceiveAsync();
                if (message == null)
                {
                    // link to IoT Hub has been closed
                    this.ShutdownOnReceiveError(context, null);
                    return;
                }

                bool receiving = this.IsInState(StateFlags.Receiving);
                int processorsInRetransmission = 0;
                bool messageSent = false;

                if (this.publishPubAckProcessor.Retransmitting)
                {
                    processorsInRetransmission++;
                    AckPendingMessageState pendingPubAck = this.publishPubAckProcessor.FirstRequestPendingAck;
                    if (pendingPubAck.MessageId.Equals(message.MessageId, StringComparison.Ordinal))
                    {
                        this.RetransmitPublishMessage(context, message, pendingPubAck);
                        messageSent = true;
                    }
                }

                if (processorsInRetransmission == 0)
                {
                    this.PublishToClientAsync(context, message).OnFault(ShutdownOnPublishFaultAction, context);
                    if (!this.IsInState(StateFlags.Closed)
                        && (this.MessagePendingAckCount < this.settings.MaxPendingOutboundMessages))
                    {
                        this.Receive(context); // todo: review for potential stack depth issues
                    }
                    else
                    {
                        this.ResetState(StateFlags.Receiving);
                    }
                }
                else
                {
                    if (receiving)
                    {
                        if (!messageSent)
                        {
                            // message id is different - "publish" this message (it actually will be enqueued for future retransmission immediately)
                            await this.PublishToClientAsync(context, message);
                        }
                        this.ResetState(StateFlags.Receiving);
                        for (int i = processorsInRetransmission - (messageSent ? 1 : 0); i > 0; i--)
                        {
                            // fire receive for processors that went into retransmission but held off receiving messages due to ongoing receive call
                            this.Receive(context); // todo: review for potential stack depth issues
                        }
                    }
                    else if (!messageSent)
                    {
                        throw new InvalidOperationException("Received a message that does not match");
                    }
                }
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

        async Task PublishToClientAsync(IChannelHandlerContext context, Message message)
        {
            try
            {
                using (message)
                {
                    if (this.settings.MaxOutboundRetransmissionEnforced && message.DeliveryCount > this.settings.MaxOutboundRetransmissionCount)
                    {
                        await this.RejectMessageAsync(message);
                        return;
                    }

                    string topicName;
                    var completeContext = new ReadOnlyMergeDictionary<string, string>(this.sessionContext, message.Properties);
                    if (!this.topicNameRouter.TryMapRouteToTopicName(RouteSourceType.Notification, completeContext, out topicName))
                    {
                        // source is not configured
                        await this.RejectMessageAsync(message);
                        return;
                    }

                    QualityOfService qos;
                    QualityOfService maxRequestedQos;
                    if (this.TryMatchSubscription(topicName, message.EnqueuedTimeUtc, out maxRequestedQos))
                    {
                        qos = Util.DeriveQos(message, this.settings);
                        if (maxRequestedQos < qos)
                        {
                            qos = maxRequestedQos;
                        }
                    }
                    else
                    {
                        // no matching subscription found - complete the message without publishing
                        await this.RejectMessageAsync(message);
                        return;
                    }

                    PublishPacket packet = await Util.ComposePublishPacketAsync(context, message, qos, topicName);
                    switch (qos)
                    {
                        case QualityOfService.AtMostOnce:
                            await this.PublishToClientQos0Async(context, message, packet);
                            break;
                        case QualityOfService.AtLeastOnce:
                            await this.PublishToClientQos1Async(context, message, packet);
                            break;
                        case QualityOfService.ExactlyOnce:
                            throw new InvalidOperationException("Requested QoS level is not supported.");
                        default:
                            throw new InvalidOperationException("Requested QoS level is not supported.");
                    }
                }
            }
            catch (Exception ex)
            {
                // todo: log more details
                ShutdownOnError(context, "<- PUBLISH", ex);
            }
        }

        async Task RejectMessageAsync(Message message)
        {
//            await this.iotHubClient.RejectAsync(message.LockToken); // awaiting guarantees that we won't complete consecutive message before this is completed.
        }

        Task PublishToClientQos0Async(IChannelHandlerContext context, Message message, PublishPacket packet)
        {
//            if (message.DeliveryCount == 0)
//            {
//                return Task.WhenAll(
//                    this.iotHubClient.CompleteAsync(message.LockToken),
//                    Util.WriteMessageAsync(context, packet));
//            }
//            else
//            {
//                return this.iotHubClient.CompleteAsync(message.LockToken);
//            }
            return Util.WriteMessageAsync(context, packet);
        }

        Task PublishToClientQos1Async(IChannelHandlerContext context, Message message, PublishPacket packet)
        {
            return this.publishPubAckProcessor.SendRequestAsync(context, packet, new AckPendingMessageState(message, packet));
        }

        async Task AcknowledgePublishAsync(IChannelHandlerContext context, AckPendingMessageState message)
        {
            this.ResumeReadingIfNecessary(context);

            // todo: is try-catch needed here?
            try
            {
                //await this.iotHubClient.CompleteAsync(message.LockToken);

                if (this.publishPubAckProcessor.ResumeRetransmission(context))
                {
                    return;
                }

                this.RestartReceiveIfPossible(context);
            }
            catch (Exception ex)
            {
                ShutdownOnError(context, "-> PUBACK", ex);
            }
        }


        void RestartReceiveIfPossible(IChannelHandlerContext context)
        {
            // restarting receive loop if was stopped due to reaching MaxOutstandingOutboundMessageCount cap
            if (!this.IsInState(StateFlags.Receiving)
                && this.MessagePendingAckCount < this.settings.MaxPendingOutboundMessages)
            {
                this.StartReceiving(context);
            }
        }

        async void RetransmitNextPublish(IChannelHandlerContext context, AckPendingMessageState messageInfo)
        {
            bool wasReceiving = this.IsInState(StateFlags.Receiving);

            try
            {
                //await this.iotHubClient.AbandonAsync(messageInfo.LockToken);

                if (!wasReceiving)
                {
                    this.Receive(context);
                }
            }
            catch (IotHubException ex)
            {
                this.ShutdownOnReceiveError(context, ex.ToString());
            }
            catch (Exception ex)
            {
                ShutdownOnError(context, ex.ToString());
            }
        }

        sealed class AckPendingMessageState : IPacketReference, ISupportRetransmission // todo: recycle?
        {
            public AckPendingMessageState(Message message, PublishPacket packet)
                : this(message.MessageId, packet.PacketId, packet.QualityOfService, message.LockToken)
            {
            }

            public AckPendingMessageState(string messageId, int packetId, QualityOfService qualityOfService, string lockToken)
            {
                this.MessageId = messageId;
                this.PacketId = packetId;
                this.QualityOfService = qualityOfService;
                this.LockToken = lockToken;
                this.SentTime = DateTime.UtcNow;
                this.StartTimestamp = PreciseTimeSpan.FromStart;
            }

            public PreciseTimeSpan StartTimestamp { get; set; }

            public string MessageId { get; private set; }

            public int PacketId { get; private set; }

            public string LockToken { get; private set; }

            public DateTime SentTime { get; private set; }

            public QualityOfService QualityOfService { get; private set; }

            public void Reset(Message message)
            {
                if (message.MessageId != this.MessageId)
                {
                    throw new InvalidOperationException(string.Format(
                        "Expected to receive message with id of {0} but saw a message with id of {1}. Protocol Gateway only supports exclusive connection to IoT Hub.",
                        this.MessageId, message.MessageId));
                }

                this.LockToken = message.LockToken;
                this.SentTime = DateTime.UtcNow;
            }

            public void ResetMessage(Message message)
            {
                if (message.MessageId != this.MessageId)
                {
                    throw new InvalidOperationException(string.Format(
                        "Expected to receive message with id of {0} but saw a message with id of {1}. Protocol Gateway only supports exclusive connection to IoT Hub.",
                        this.MessageId, message.MessageId));
                }

                this.LockToken = message.LockToken;
            }

            public void ResetSentTime()
            {
                this.SentTime = DateTime.UtcNow;
            }
        }

        async void RetransmitPublishMessage(IChannelHandlerContext context, Message message, AckPendingMessageState messageInfo)
        {
            try
            {
                using (message)
                {
                    string topicName;
                    var completeContext = new ReadOnlyMergeDictionary<string, string>(this.sessionContext, message.Properties);
                    if (!this.topicNameRouter.TryMapRouteToTopicName(RouteSourceType.Notification, completeContext, out topicName))
                    {
                        throw new InvalidOperationException("Route mapping failed on retransmission.");
                    }

                    PublishPacket packet = await Util.ComposePublishPacketAsync(context, message, messageInfo.QualityOfService, topicName);

                    messageInfo.ResetMessage(message);
                    await this.publishPubAckProcessor.RetransmitAsync(context, packet, messageInfo);
                }
            }
            catch (Exception ex)
            {
                // todo: log more details
                ShutdownOnError(context, "<- PUBLISH (retransmission)", ex);
            }
        }

        public enum RouteSourceType
        {
            Unknown,
            Notification
        }

        bool TryMatchSubscription(string topicName, DateTime messageTime, out QualityOfService qos)
        {
            bool found = false;
            qos = QualityOfService.AtMostOnce;
            foreach (Subscription subscription in this.sessionState.Subscriptions)
            {
                if ((!found || subscription.QualityOfService > qos)
                    && subscription.CreationTime < messageTime
                    && Util.CheckTopicFilterMatch(topicName, subscription.TopicFilter))
                {
                    found = true;
                    qos = subscription.QualityOfService;
                    if (qos >= this.maxSupportedQos)
                    {
                        qos = this.maxSupportedQos;
                        break;
                    }
                }
            }
            return found;
        }

        void ShutdownOnReceiveError(IChannelHandlerContext context, string exception)
        {
            this.publishPubAckProcessor.Abort();
            this.publishProcessor.Abort();

            ShutdownOnError(context, "Receive", exception);
        }

        #endregion

        #region CONNECT handling and lifecycle management

        /// <summary>
        ///     Performs complete initialization of <see cref="MqttIotHubAdapter" /> based on received CONNECT packet.
        /// </summary>
        /// <param name="context"><see cref="IChannelHandlerContext" /> instance.</param>
        /// <param name="packet">CONNECT packet.</param>
        async void AcknowledgeConnection(IChannelHandlerContext context, ConnAckPacket packet)
        {
            bool connAckSent = false;

            Exception exception = null;
            try
            {
                if (!this.IsInState(StateFlags.Connecting))
                {
                    ShutdownOnError(context, "CONNECT has been received in current session already. Only one CONNECT is expected per session.");
                    return;
                }

                this.stateFlags = StateFlags.Connected;
                
                bool sessionPresent = await this.EstablishSessionStateAsync(this.deviceId, this.settings.CleanSession);

                this.keepAliveTimeout = this.DeriveKeepAliveTimeout(packet);

                this.sessionContext = new Dictionary<string, string>
                {
                    { DeviceIdParam, this.deviceId }
                };

                this.StartReceiving(context);

                connAckSent = true;
                await Util.WriteMessageAsync(context, new ConnAckPacket
                {
                    SessionPresent = sessionPresent,
                    ReturnCode = ConnectReturnCode.Accepted
                });

                this.CompleteConnect(context);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            if (exception != null)
            {
                if (!connAckSent)
                {
                    try
                    {
                        await Util.WriteMessageAsync(context, new ConnAckPacket
                        {
                            ReturnCode = ConnectReturnCode.RefusedServerUnavailable
                        });
                    }
                    catch (Exception ex)
                    {
                        
                    }
                }

                ShutdownOnError(context, "CONNECT", exception);
            }
        }

        /// <summary>
        ///     Loads and updates (as necessary) session state.
        /// </summary>
        /// <param name="clientId">Client identificator to load the session state for.</param>
        /// <param name="cleanSession">Determines whether session has to be deleted if it already exists.</param>
        /// <returns></returns>
        async Task<bool> EstablishSessionStateAsync(string clientId, bool cleanSession)
        {
            ISessionState existingSessionState = await this.sessionStateManager.GetAsync(clientId);
            if (cleanSession)
            {
                if (existingSessionState != null)
                {
                    await this.sessionStateManager.DeleteAsync(clientId, existingSessionState);
                    // todo: loop in case of concurrent access? how will we resolve conflict with concurrent connections?
                }

                this.sessionState = this.sessionStateManager.Create(true);
                return false;
            }
            else
            {
                if (existingSessionState == null)
                {
                    this.sessionState = this.sessionStateManager.Create(false);
                    return false;
                }
                else
                {
                    this.sessionState = existingSessionState;
                    return true;
                }
            }
        }

        TimeSpan DeriveKeepAliveTimeout(ConnectPacket packet)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(packet.KeepAliveInSeconds * 1.5);
            TimeSpan? maxTimeout = this.settings.MaxKeepAliveTimeout;
            if (maxTimeout.HasValue && (timeout > maxTimeout.Value || timeout == TimeSpan.Zero))
            {
                return maxTimeout.Value;
            }

            return timeout;
        }

        /// <summary>
        ///     Finalizes initialization based on CONNECT packet: dispatches keep-alive timer and releases messages buffered before
        ///     the CONNECT processing was finalized.
        /// </summary>
        /// <param name="context"><see cref="IChannelHandlerContext" /> instance.</param>
        void CompleteConnect(IChannelHandlerContext context)
        {
            if (this.keepAliveTimeout > TimeSpan.Zero)
            {
                PingServer(context);
            }

            this.stateFlags = StateFlags.Connected;

            if (this.connectWithSubscribeQueue != null)
            {
                while (this.connectWithSubscribeQueue.Count > 0)
                {
                    Packet packet = this.connectWithSubscribeQueue.Dequeue();
                    this.ProcessMessage(context, packet);
                }
                this.connectWithSubscribeQueue = null; // release unnecessary queue
            }
        }

        static void CheckConnectionTimeout(object state)
        {
            var context = (IChannelHandlerContext)state;
            var handler = (MqttIotHubAdapter)context.Handler;
            if (handler.IsInState(StateFlags.Connecting))
            {
                ShutdownOnError(context, "Connection timed out on waiting for CONACK packet from server.");
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

        static void ShutdownOnError(IChannelHandlerContext context, string scope, Exception exception)
        {
            ShutdownOnError(context, scope, exception.ToString());
        }

        static void ShutdownOnError(IChannelHandlerContext context, string scope, string exception)
        {
            ShutdownOnError(context, string.Format("Exception occured ({0}): {1}", scope, exception));
        }

        /// <summary>
        ///     Logs error and initiates closure of both channel and hub connection.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="reason">Explanation for channel closure.</param>
        static void ShutdownOnError(IChannelHandlerContext context, string reason)
        {
            Contract.Requires(!string.IsNullOrEmpty(reason));

            var self = (MqttIotHubAdapter)context.Handler;
            if (!self.IsInState(StateFlags.Closed))
            {
                self.Shutdown(context, false);
            }
        }

        /// <summary>
        ///     Closes channel
        /// </summary>
        /// <param name="context"></param>
        /// <param name="graceful"></param>
        async void Shutdown(IChannelHandlerContext context, bool graceful)
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
                this.publishProcessor.Complete();
                this.publishPubAckProcessor.Complete();
                await Task.WhenAll(
                    this.publishProcessor.Completion,
                    this.publishPubAckProcessor.Completion);
            }
            catch
            {
                // ignored on closing
            }
        }

        #endregion

        #region helper methods

        static Action<Task> CreateScopedFaultAction(string scope)
        {
            return task =>
            {
                // ReSharper disable once PossibleNullReferenceException // called in case of fault only, so task.Exception is never null
                var ex = task.Exception.InnerException as ChannelMessageProcessingException;
                if (ex != null)
                {
                    ShutdownOnError(ex.Context, scope, task.Exception);
                }
            };
        }

        void StartReceiving(IChannelHandlerContext context)
        {
            this.stateFlags |= StateFlags.Receiving;
            this.Receive(context);
        }

        bool IsInState(StateFlags stateFlagsToCheck)
        {
            return (this.stateFlags & stateFlagsToCheck) == stateFlagsToCheck;
        }

        bool IsInStates(StateFlags stateFlagsToCheck)
        {
            return (this.stateFlags & stateFlagsToCheck) > 0;
        }

        bool IsStateCompleted(StateFlags stateFlagsToCheck)
        {
            return this.stateFlags > stateFlagsToCheck;
        }

        void ResetState(StateFlags stateFlagsToReset)
        {
            this.stateFlags &= ~stateFlagsToReset;
        }

        #endregion

        [Flags]
        enum StateFlags
        {
            NotConnected = 1,
            Connecting = 1 << 1,
            Connected = 1 << 2,
            ChangingSubscriptions = 1 << 3,
            Receiving = 1 << 4,
            Closed = 1 << 5
        }

        public Task SendEventAsync(Message repeat)
        {
            throw new NotImplementedException();
        }

        public Task SendEventAsync(Message message, IDictionary<string, string> properties)
        {
            throw new NotImplementedException();
        }

        public Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}