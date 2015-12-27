// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.ProtocolGateway.Mqtt
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;
    using DotNetty.Buffers;
    using DotNetty.Codecs.Mqtt.Packets;
    using DotNetty.Common;
    using DotNetty.Common.Utilities;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Channels;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;

    internal sealed class MqttIotHubAdapter : ChannelHandlerAdapter
    {
        const string UnmatchedFlagPropertyName = "Unmatched";
        const string SubjectPropertyName = "Subject";
        const string DeviceIdParam = "deviceId";

        static readonly Action<object> CheckConnectTimeoutCallback = CheckConnectionTimeout;
        static readonly Action<object> CheckKeepAliveCallback = CheckKeepAlive;
        static readonly Action<Task, object> ShutdownOnWriteFaultAction = (task, ctx) => ShutdownOnError((IChannelHandlerContext)ctx, "WriteAndFlushAsync", task.Exception);
        static readonly Action<Task, object> ShutdownOnPublishFaultAction = (task, ctx) => ShutdownOnError((IChannelHandlerContext)ctx, "<- PUBLISH", task.Exception);
        static readonly Action<Task> ShutdownOnPublishToServerFaultAction = CreateScopedFaultAction("-> PUBLISH");
        static readonly Action<Task> ShutdownOnPubAckFaultAction = CreateScopedFaultAction("-> PUBACK");

        readonly Settings settings;
        StateFlags stateFlags;
        DateTime lastClientActivityTime;
        ISessionState sessionState;
        readonly PacketAsyncProcessor<PublishPacket> publishProcessor;
        readonly RequestAckPairProcessor<AckPendingMessageState, PublishPacket> publishPubAckProcessor;
        readonly ITopicNameRouter topicNameRouter;
        Dictionary<string, string> sessionContext;
        Identity identity;
        readonly QualityOfService maxSupportedQosToClient;
        TimeSpan keepAliveTimeout;
        readonly ISessionStatePersistenceProvider sessionStateManager;
        readonly IAuthenticationProvider authProvider;
        Queue<Packet> connectWithSubscribeQueue;

        public MqttIotHubAdapter(Settings settings, ISessionStatePersistenceProvider sessionStateManager, IAuthenticationProvider authProvider,
            ITopicNameRouter topicNameRouter)
        {
            Contract.Requires(settings != null);
            Contract.Requires(sessionStateManager != null);
            Contract.Requires(authProvider != null);
            Contract.Requires(topicNameRouter != null);

            this.maxSupportedQosToClient = QualityOfService.AtLeastOnce;
            this.settings = settings;
            this.sessionStateManager = sessionStateManager;
            this.authProvider = authProvider;
            this.topicNameRouter = topicNameRouter;

            this.publishProcessor = new PacketAsyncProcessor<PublishPacket>(this.PublishToServerAsync);
            this.publishProcessor.Completion.OnFault(ShutdownOnPublishToServerFaultAction);

            TimeSpan? ackTimeout = this.settings.DeviceReceiveAckCanTimeout ? this.settings.DeviceReceiveAckTimeout : (TimeSpan?)null;
            this.publishPubAckProcessor = new RequestAckPairProcessor<AckPendingMessageState, PublishPacket>(this.AcknowledgePublishAsync, this.RetransmitNextPublish, ackTimeout);
            this.publishPubAckProcessor.Completion.OnFault(ShutdownOnPubAckFaultAction);
        }

        Queue<Packet> SubscriptionChangeQueue
        {
            get { return this.subscriptionChangeQueue ?? (this.subscriptionChangeQueue = new Queue<Packet>(4)); }
        }

        Queue<Packet> ConnectPendingQueue
        {
            get { return this.connectWithSubscribeQueue ?? (this.connectWithSubscribeQueue = new Queue<Packet>(4)); }
        }

        bool ConnectedToHub
        {
            get { return this.iotHubClient != null; }
        }

        int InboundBacklogSize
        {
            get
            {
                return this.publishProcessor.BacklogSize
                    + this.publishPubAckProcessor.BacklogSize
            }
        }

        int MessagePendingAckCount
        {
            get
            {
                return this.publishPubAckProcessor.RequestPendingAckCount
            }
        }

        #region IChannelHandler overrides

        public override void ChannelActive(IChannelHandlerContext context)
        {
            this.stateFlags = StateFlags.WaitingForConnect;
            TimeSpan? timeout = this.settings.ConnectArrivalTimeout;
            if (timeout.HasValue)
            {
                context.Channel.EventLoop.ScheduleAsync(CheckConnectTimeoutCallback, context, timeout.Value);
            }
            base.ChannelActive(context);

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
                if (this.IsInState(StateFlags.ProcessingConnect))
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
                case PacketType.CONNECT:
                    this.Connect(context, (ConnectPacket)packet);
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
                                acks.Add(Util.AddSubscriptions(newState, (SubscribePacket)packet, this.maxSupportedQosToClient));
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
                        await this.sessionStateManager.SetAsync(this.identity.ToString(), newState);
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
            if (!this.ConnectedToHub)
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

                await this.iotHubClient.SendAsync(message);
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
                    if (!this.identity.DeviceId.Equals(messageDeviceId, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException(
                            string.Format("Device ID provided in topic name ({0}) does not match ID of the device publishing message ({1}). IoT Hub Name: {2}",
                            messageDeviceId, this.identity.DeviceId, this.identity.IoTHubHostName));
                    }
                    message.Properties.Remove(DeviceIdParam);
                }
            }
            else
            {
                if (MqttIotHubAdapterEventSource.Log.IsWarningEnabled)
                {
                    MqttIotHubAdapterEventSource.Log.Warning("Topic name could not be matched against any of the configured routes. Falling back to default telemetry settings.", packet.ToString());
                }
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
                Message message = await this.iotHubClient.ReceiveAsync();
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
            await this.iotHubClient.RejectAsync(message.LockToken); // awaiting guarantees that we won't complete consecutive message before this is completed.
        }

        Task PublishToClientQos0Async(IChannelHandlerContext context, Message message, PublishPacket packet)
        {
            if (message.DeliveryCount == 0)
            {
                return Task.WhenAll(
                    this.iotHubClient.CompleteAsync(message.LockToken),
                    Util.WriteMessageAsync(context, packet));
            }
            else
            {
                return this.iotHubClient.CompleteAsync(message.LockToken);
            }
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
                await this.iotHubClient.CompleteAsync(message.LockToken);

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
                await this.iotHubClient.AbandonAsync(messageInfo.LockToken);

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
                    if (qos >= this.maxSupportedQosToClient)
                    {
                        qos = this.maxSupportedQosToClient;
                        break;
                    }
                }
            }
            return found;
        }

        async void ShutdownOnReceiveError(IChannelHandlerContext context, string exception)
        {
            this.publishPubAckProcessor.Abort();
            this.publishProcessor.Abort();

            IDeviceClient hub = this.iotHubClient;
            if (hub != null)
            {
                this.iotHubClient = null;
                try
                {
                    await hub.DisposeAsync();
                }
                catch (Exception ex)
                {
                }
            }
            ShutdownOnError(context, "Receive", exception);
        }

        #endregion

        #region CONNECT handling and lifecycle management

        /// <summary>
        ///     Performs complete initialization of <see cref="MqttIotHubAdapter" /> based on received CONNECT packet.
        /// </summary>
        /// <param name="context"><see cref="IChannelHandlerContext" /> instance.</param>
        /// <param name="packet">CONNECT packet.</param>
        async void Connect(IChannelHandlerContext context, ConnectPacket packet)
        {
            bool connAckSent = false;

            Exception exception = null;
            try
            {
                if (!this.IsInState(StateFlags.WaitingForConnect))
                {
                    ShutdownOnError(context, "CONNECT has been received in current session already. Only one CONNECT is expected per session.");
                    return;
                }

                this.stateFlags = StateFlags.ProcessingConnect;
                AuthenticationResult authResult = await this.authProvider.AuthenticateAsync(packet.ClientId,
                    packet.Username, packet.Password, context.Channel.RemoteAddress);
                if (authResult == null)
                {
                    connAckSent = true;
                    await Util.WriteMessageAsync(context, new ConnAckPacket
                    {
                        ReturnCode = ConnectReturnCode.RefusedNotAuthorized
                    });
                    ShutdownOnError(context, "Authentication failed.");
                    return;
                }

                this.identity = authResult.Identity;

                this.iotHubClient = await this.deviceClientFactory(authResult);

                bool sessionPresent = await this.EstablishSessionStateAsync(this.identity.ToString(), packet.CleanSession);

                this.keepAliveTimeout = this.DeriveKeepAliveTimeout(packet);

                if (packet.HasWill)
                {
                    var will = new PublishPacket(packet.WillQualityOfService, false, packet.WillRetain);
                    will.TopicName = packet.WillTopicName;
                    will.Payload = packet.WillMessage;
                    this.willPacket = will;
                }

                this.sessionContext = new Dictionary<string, string>
                {
                    { DeviceIdParam, this.identity.DeviceId }
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
                CheckKeepAlive(context);
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
            if (handler.IsInState(StateFlags.WaitingForConnect))
            {
                ShutdownOnError(context, "Connection timed out on waiting for CONNECT packet from client.");
            }
        }

        static void CheckKeepAlive(object ctx)
        {
            var context = (IChannelHandlerContext)ctx;
            var self = (MqttIotHubAdapter)context.Handler;
            TimeSpan elapsedSinceLastActive = DateTime.UtcNow - self.lastClientActivityTime;
            if (elapsedSinceLastActive > self.keepAliveTimeout)
            {
                ShutdownOnError(context, "Keep Alive timed out.");
                return;
            }

            context.Channel.EventLoop.ScheduleAsync(CheckKeepAliveCallback, context, self.keepAliveTimeout - elapsedSinceLastActive);
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
            if (!this.ConnectedToHub)
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

                IDeviceClient hub = this.iotHubClient;
                this.iotHubClient = null;
                await hub.DisposeAsync();
            }
            catch (Exception ex)
            {
                
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

        void ResetState(StateFlags stateFlagsToReset)
        {
            this.stateFlags &= ~stateFlagsToReset;
        }

        #endregion

        [Flags]
        enum StateFlags
        {
            WaitingForConnect = 1,
            ProcessingConnect = 1 << 1,
            Connected = 1 << 2,
            ChangingSubscriptions = 1 << 3,
            Receiving = 1 << 4,
            Closed = 1 << 5
        }
    }

    public static class TaskExtensions
    {
        public static void OnFault(this Task task, Action<Task> faultAction)
        {
            switch (task.Status)
            {
                case TaskStatus.RanToCompletion:
                case TaskStatus.Canceled:
                    break;
                case TaskStatus.Faulted:
                    faultAction(task);
                    break;
                default:
                    task.ContinueWith(faultAction, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
                    break;
            }
        }

        public static void OnFault(this Task task, Action<Task, object> faultAction, object state)
        {
            switch (task.Status)
            {
                case TaskStatus.RanToCompletion:
                case TaskStatus.Canceled:
                    break;
                case TaskStatus.Faulted:
                    faultAction(task, state);
                    break;
                default:
                    task.ContinueWith(faultAction, state, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
                    break;
            }
        }
    }

    static class Util
    {
        const char SegmentSeparatorChar = '/';
        const char SingleSegmentWildcardChar = '+';
        const char MultiSegmentWildcardChar = '#';
        static readonly char[] WildcardChars = { MultiSegmentWildcardChar, SingleSegmentWildcardChar };
        const string IotHubTrueString = "true";

        public static bool CheckTopicFilterMatch(string topicName, string topicFilter)
        {
            int topicFilterIndex = 0;
            int topicNameIndex = 0;
            while (topicNameIndex < topicName.Length && topicFilterIndex < topicFilter.Length)
            {
                int wildcardIndex = topicFilter.IndexOfAny(WildcardChars, topicFilterIndex);
                if (wildcardIndex == -1)
                {
                    int matchLength = Math.Max(topicFilter.Length - topicFilterIndex, topicName.Length - topicNameIndex);
                    return string.Compare(topicFilter, topicFilterIndex, topicName, topicNameIndex, matchLength, StringComparison.Ordinal) == 0;
                }
                else
                {
                    if (topicFilter[wildcardIndex] == MultiSegmentWildcardChar)
                    {
                        if (wildcardIndex == 0) // special case -- any topic name would match
                        {
                            return true;
                        }
                        else
                        {
                            int matchLength = wildcardIndex - topicFilterIndex - 1;
                            if (string.Compare(topicFilter, topicFilterIndex, topicName, topicNameIndex, matchLength, StringComparison.Ordinal) == 0
                                && (topicName.Length == topicNameIndex + matchLength || (topicName.Length > topicNameIndex + matchLength && topicName[topicNameIndex + matchLength] == SegmentSeparatorChar)))
                            {
                                // paths match up till wildcard and either it is parent topic in hierarchy (one level above # specified) or any child topic under a matching parent topic
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }
                    else
                    {
                        // single segment wildcard
                        int matchLength = wildcardIndex - topicFilterIndex;
                        if (matchLength > 0 && string.Compare(topicFilter, topicFilterIndex, topicName, topicNameIndex, matchLength, StringComparison.Ordinal) != 0)
                        {
                            return false;
                        }
                        topicNameIndex = topicName.IndexOf(SegmentSeparatorChar, topicNameIndex + matchLength);
                        topicFilterIndex = wildcardIndex + 1;
                        if (topicNameIndex == -1)
                        {
                            // there's no more segments following matched one
                            return topicFilterIndex == topicFilter.Length;
                        }
                    }
                }
            }

            return topicFilterIndex == topicFilter.Length && topicNameIndex == topicName.Length;
        }

        public static QualityOfService DeriveQos(Message message, Settings config)
        {
            QualityOfService qos;
            string qosValue;
            if (message.Properties.TryGetValue(config.QoSPropertyName, out qosValue))
            {
                int qosAsInt;
                if (int.TryParse(qosValue, out qosAsInt))
                {
                    qos = (QualityOfService)qosAsInt;
                    if (qos > QualityOfService.ExactlyOnce)
                    {
                        qos = config.DefaultPublishToClientQoS;
                    }
                }
                else
                {
                    qos = config.DefaultPublishToClientQoS;
                }
            }
            else
            {
                qos = config.DefaultPublishToClientQoS;
            }
            return qos;
        }

        public static Message CompleteMessageFromPacket(Message message, PublishPacket packet, Settings settings)
        {
            message.MessageId = Guid.NewGuid().ToString("N");
            if (packet.RetainRequested)
            {
                message.Properties[settings.RetainPropertyName] = IotHubTrueString;
            }
            if (packet.Duplicate)
            {
                message.Properties[settings.DupPropertyName] = IotHubTrueString;
            }

            return message;
        }

        public static async Task<PublishPacket> ComposePublishPacketAsync(IChannelHandlerContext context, Message message,
            QualityOfService qos, string topicName)
        {
            bool duplicate = message.DeliveryCount > 0;

            var packet = new PublishPacket(qos, duplicate, false);
            packet.TopicName = topicName;
            if (qos > QualityOfService.AtMostOnce)
            {
                int packetId = unchecked((ushort)message.SequenceNumber);
                switch (qos)
                {
                    case QualityOfService.AtLeastOnce:
                        packetId &= 0x7FFF; // clear 15th bit
                        break;
                    case QualityOfService.ExactlyOnce:
                        packetId |= 0x8000; // set 15th bit
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("qos", qos, null);
                }
                packet.PacketId = packetId;
            }
            using (Stream payloadStream = message.GetBodyStream())
            {
                long streamLength = payloadStream.Length;
                if (streamLength > int.MaxValue)
                {
                    throw new InvalidOperationException(string.Format("Message size ({0} bytes) is too big to process.", streamLength));
                }

                int length = (int)streamLength;
                IByteBuffer buffer = context.Channel.Allocator.Buffer(length, length);
                await buffer.WriteBytesAsync(payloadStream, length);
                Contract.Assert(buffer.ReadableBytes == length);

                packet.Payload = buffer;
            }
            return packet;
        }

        internal static IAuthenticationMethod DeriveAuthenticationMethod(IAuthenticationMethod currentAuthenticationMethod, AuthenticationResult authenticationResult)
        {
            string deviceId = authenticationResult.Identity.DeviceId;
            switch (authenticationResult.Properties.Scope)
            {
                case AuthenticationScope.None:
                    var policyKeyAuth = currentAuthenticationMethod as DeviceAuthenticationWithSharedAccessPolicyKey;
                    if (policyKeyAuth != null)
                    {
                        return new DeviceAuthenticationWithSharedAccessPolicyKey(deviceId, policyKeyAuth.PolicyName, policyKeyAuth.Key);
                    }
                    var deviceKeyAuth = currentAuthenticationMethod as DeviceAuthenticationWithRegistrySymmetricKey;
                    if (deviceKeyAuth != null)
                    {
                        return new DeviceAuthenticationWithRegistrySymmetricKey(deviceId, deviceKeyAuth.DeviceId);
                    }
                    var deviceTokenAuth = currentAuthenticationMethod as DeviceAuthenticationWithToken;
                    if (deviceTokenAuth != null)
                    {
                        return new DeviceAuthenticationWithToken(deviceId, deviceTokenAuth.Token);
                    }
                    throw new InvalidOperationException("");
                case AuthenticationScope.SasToken:
                    return new DeviceAuthenticationWithToken(deviceId, authenticationResult.Properties.Secret);
                case AuthenticationScope.DeviceKey:
                    return new DeviceAuthenticationWithRegistrySymmetricKey(deviceId, authenticationResult.Properties.Secret);
                case AuthenticationScope.HubKey:
                    return new DeviceAuthenticationWithSharedAccessPolicyKey(deviceId, authenticationResult.Properties.PolicyName, authenticationResult.Properties.Secret);
                default:
                    throw new InvalidOperationException("Unexpected AuthenticationScope value: " + authenticationResult.Properties.Scope);
            }
        }

        public static SubAckPacket AddSubscriptions(ISessionState session, SubscribePacket packet, QualityOfService maxSupportedQos)
        {
            List<Subscription> subscriptions = session.Subscriptions;
            var returnCodes = new List<QualityOfService>(subscriptions.Count);
            foreach (SubscriptionRequest request in packet.Requests)
            {
                Subscription existingSubscription = null;
                for (int i = subscriptions.Count - 1; i >= 0; i--)
                {
                    Subscription subscription = subscriptions[i];
                    if (subscription.TopicFilter.Equals(request.TopicFilter, StringComparison.Ordinal))
                    {
                        subscriptions.RemoveAt(i);
                        existingSubscription = subscription;
                        break;
                    }
                }

                QualityOfService finalQos = request.QualityOfService < maxSupportedQos ? request.QualityOfService : maxSupportedQos;

                subscriptions.Add(existingSubscription == null
                    ? new Subscription(request.TopicFilter, request.QualityOfService)
                    : existingSubscription.CreateUpdated(finalQos));

                returnCodes.Add(finalQos);
            }
            var ack = new SubAckPacket
            {
                PacketId = packet.PacketId,
                ReturnCodes = returnCodes
            };
            return ack;
        }

        public static UnsubAckPacket RemoveSubscriptions(ISessionState session, UnsubscribePacket packet)
        {
            List<Subscription> subscriptions = session.Subscriptions;
            foreach (string topicToRemove in packet.TopicFilters)
            {
                for (int i = subscriptions.Count - 1; i >= 0; i--)
                {
                    if (subscriptions[i].TopicFilter.Equals(topicToRemove, StringComparison.Ordinal))
                    {
                        subscriptions.RemoveAt(i);
                        break;
                    }
                }
            }
            var ack = new UnsubAckPacket
            {
                PacketId = packet.PacketId
            };
            return ack;
        }

        public static async Task WriteMessageAsync(IChannelHandlerContext context, object message)
        {
            await context.WriteAndFlushAsync(message);
        }
    }

    public enum AuthenticationScope
    {
        None,
        SasToken,
        DeviceKey,
        HubKey
    }

    
    public sealed class AuthenticationProperties
    {
        public static AuthenticationProperties SuccessWithSasToken(string token)
        {
            return new AuthenticationProperties
            {
                Scope = AuthenticationScope.SasToken,
                Secret = token,
            };
        }

        public static AuthenticationProperties SuccessWithHubKey(string keyName, string keyValue)
        {
            return new AuthenticationProperties
            {
                Scope = AuthenticationScope.HubKey,
                PolicyName = keyName,
                Secret = keyValue
            };
        }

        public static AuthenticationProperties SuccessWithDeviceKey(string keyValue)
        {
            return new AuthenticationProperties
            {
                Scope = AuthenticationScope.DeviceKey,
                Secret = keyValue
            };
        }

        public static AuthenticationProperties SuccessWithDefaultCredentials()
        {
            return new AuthenticationProperties
            {
                Scope = AuthenticationScope.None
            };
        }

        AuthenticationProperties()
        {
        }

        public string PolicyName { get; private set; }

        public string Secret { get; private set; }

        public AuthenticationScope Scope { get; private set; }
    }

    public sealed class AuthenticationResult
    {
        public static AuthenticationResult SuccessWithSasToken(Identity identity, string token)
        {
            return new AuthenticationResult
            {
                Properties = AuthenticationProperties.SuccessWithSasToken(token),
                Identity = identity
            };
        }

        public static AuthenticationResult SuccessWithHubKey(Identity identity, string keyName, string keyValue)
        {
            return new AuthenticationResult
            {
                Identity = identity,
                Properties = AuthenticationProperties.SuccessWithHubKey(keyName, keyValue)
            };
        }

        public static AuthenticationResult SuccessWithDeviceKey(Identity identity, string keyValue)
        {
            return new AuthenticationResult
            {
                Identity = identity,
                Properties = AuthenticationProperties.SuccessWithDeviceKey(keyValue)
            };
        }

        public static AuthenticationResult SuccessWithDefaultCredentials(Identity identity)
        {
            return new AuthenticationResult
            {
                Identity = identity,
                Properties = AuthenticationProperties.SuccessWithDefaultCredentials()
            };
        }

        AuthenticationResult()
        {
        }

        public AuthenticationProperties Properties { get; private set; }

        public Identity Identity { get; private set; }
    }

    public sealed class Identity
    {
        public string IoTHubHostName { get; private set; }

        public string DeviceId { get; private set; }

        public Identity(string iotHubHostName, string deviceId)
        {
            this.IoTHubHostName = iotHubHostName;
            this.DeviceId = deviceId;
        }

        public override string ToString()
        {
            return this.IoTHubHostName + "/" + this.DeviceId;
        }

        public static Identity Parse(string username)
        {
            int delimiterPos = username.IndexOf("/", StringComparison.Ordinal);
            if (delimiterPos < 0)
            {
                throw new FormatException(string.Format("Invalid username format: {0}", username));
            }
            string iotHubName = username.Substring(0, delimiterPos);
            string deviceId = username.Substring(delimiterPos + 1);

            return new Identity(iotHubName, deviceId);
        }
    }

    internal interface IAuthenticationProvider
    {
        Task<AuthenticationResult> AuthenticateAsync(string clientId, string username, string password, EndPoint clientAddress);
    }

    sealed class CompletionPendingMessageState : IPacketReference, ISupportRetransmission
    {
        public CompletionPendingMessageState(int packetId, string lockToken,
             IQos2MessageDeliveryState deliveryState, PreciseTimeSpan startTimestamp)
        {
            this.PacketId = packetId;
            this.LockToken = lockToken;
            this.DeliveryState = deliveryState;
            this.StartTimestamp = startTimestamp;
            this.SentTime = DateTime.UtcNow;
        }

        public int PacketId { get; private set; }

        public string LockToken { get; private set; }

        public IQos2MessageDeliveryState DeliveryState { get; private set; }

        public PreciseTimeSpan StartTimestamp { get; private set; }

        public DateTime SentTime { get; private set; }

        public void ResetSentTime()
        {
            this.SentTime = DateTime.UtcNow;
        }
    }


    public interface IQos2MessageDeliveryState
    {
        DateTime LastModified { get; }

        string MessageId { get; }
    }

    sealed class ReadOnlyMergeDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        readonly IDictionary<TKey, TValue> mainDictionary;
        readonly IDictionary<TKey, TValue> secondaryDictionary;

        public ReadOnlyMergeDictionary(IDictionary<TKey, TValue> mainDictionary, IDictionary<TKey, TValue> secondaryDictionary)
        {
            Contract.Requires(mainDictionary != null);
            Contract.Requires(secondaryDictionary != null);

            this.mainDictionary = mainDictionary;
            this.secondaryDictionary = secondaryDictionary;
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            foreach (KeyValuePair<TKey, TValue> pair in this.mainDictionary)
            {
                yield return pair;
            }

            foreach (KeyValuePair<TKey, TValue> pair in this.secondaryDictionary)
            {
                if (!this.mainDictionary.ContainsKey(pair.Key))
                {
                    yield return pair;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            throw new NotSupportedException();
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Clear()
        {
            throw new NotSupportedException();
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            throw new NotSupportedException();
        }

        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            throw new NotSupportedException();
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            throw new NotSupportedException();
        }

        int ICollection<KeyValuePair<TKey, TValue>>.Count
        {
            get { throw new NotSupportedException(); }
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly
        {
            get { return true; }
        }

        bool IDictionary<TKey, TValue>.ContainsKey(TKey key)
        {
            if (this.mainDictionary.ContainsKey(key))
            {
                return true;
            }
            IDictionary<TKey, TValue> msgContext = this.secondaryDictionary;
            return msgContext != null && msgContext.ContainsKey(key);
        }

        void IDictionary<TKey, TValue>.Add(TKey key, TValue value)
        {
            throw new NotSupportedException();
        }

        bool IDictionary<TKey, TValue>.Remove(TKey key)
        {
            throw new NotSupportedException();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (this.mainDictionary.TryGetValue(key, out value))
            {
                return true;
            }
            return this.secondaryDictionary.TryGetValue(key, out value);
        }

        TValue IDictionary<TKey, TValue>.this[TKey key]
        {
            get
            {
                TValue value;
                if (!this.TryGetValue(key, out value))
                {
                    throw new ArgumentException("Specified key was not found in the dictionary.", "key");
                }
                return value;
            }
            set { throw new NotSupportedException(); }
        }

        ICollection<TKey> IDictionary<TKey, TValue>.Keys
        {
            get { throw new NotSupportedException(); }
        }

        ICollection<TValue> IDictionary<TKey, TValue>.Values
        {
            get { throw new NotSupportedException(); }
        }

    }


    public interface ITopicNameRouter
    {
        bool TryMapRouteToTopicName(MqttIotHubAdapter.RouteSourceType routeType, IDictionary<string, string> context, out string topicName);

        bool TryMapTopicNameToRoute(string topicName, out RouteDestinationType routeType, IDictionary<string, string> contextOutput);
    }
    
    public enum RouteDestinationType
    {
        Unknown,
        Telemetry
    }
}