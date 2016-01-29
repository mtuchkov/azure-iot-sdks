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
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Channels;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Common;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt.Store;

    sealed class MqttIotHubAdapter : ChannelHandlerAdapter
    {
        const string TelemetryTopicFilterFormat = "devices/{0}/messages/devicebound/#";

        static readonly Action<object> PingServerCallback = PingServer;
        static readonly Action<object> CheckConackTimeoutCallback = CheckConnectionTimeout;
        static readonly Func<IChannelHandlerContext, Exception, bool> ShutdownOnWriteErrorHandler = (ctx, ex) =>
        {
            ShutdownOnError(ctx, "WriteAndFlushAsync", ex);
            return false;
        };
        static readonly ThreadLocal<Random> ThreadLocalRandom = new ThreadLocal<Random>(() => new Random((int)DateTime.UtcNow.ToFileTimeUtc()));

        StateFlags stateFlags;

        readonly MqttTransportSettings mqttTransportSettings;
        readonly ITopicNameRouter topicNameRouter;
        readonly IWillMessageProvider willMessageProvider;
        readonly Action<MqttActionResult> connectCallback;
        readonly Action<MqttActionResult> disconnectCallback;
        readonly Action<MqttMessageReceivedResult> onMessageReceived;
        readonly ISessionStatePersistenceProvider sessionStatePersistenceProvider;
        
        readonly string deviceId;
        readonly string iotHubHostName;
        readonly string password;
        readonly TimeSpan? keepAliveTimeout;
        readonly TimeSpan pingRequestTimeout;
        QualityOfService maxSupportedQos;
        DateTime lastClientActivityTime;
        readonly Dictionary<string, string> sessionContext;

        readonly SimpleWorkQueue<PublishWorkItem> serviceBoundPublishProcessor;
        readonly OrderedTwoPhaseWorkQueue<int, PublishWorkItem> serviceBoundPubAckProcessor;
        
        readonly SimpleWorkQueue<PublishPacket> deviceBoundPublishProcessor;
        readonly OrderedTwoPhaseWorkQueue<int, PublishPacket> deviceBoundPubAckProcessor;
        
        ISessionState sessionState;

        int InboundBacklogSize { get { return this.deviceBoundPublishProcessor.BacklogSize + this.deviceBoundPubAckProcessor.BacklogSize; } }
        
        public MqttIotHubAdapter(
            string deviceId, 
            string iotHubHostName, 
            string password, 
            MqttTransportSettings mqttTransportSettings, 
            ISessionStatePersistenceProvider sessionStatePersistenceProvider, 
            ITopicNameRouter topicNameRouter, 
            IWillMessageProvider willMessageProvider,
            Action<MqttActionResult> connectCallback,
            Action<MqttActionResult> disconnectCallback,
            Action<MqttMessageReceivedResult> onMessageReceived)
        {
            Contract.Requires(deviceId != null);
            Contract.Requires(iotHubHostName != null);
            Contract.Requires(password != null);
            Contract.Requires(mqttTransportSettings != null);
            Contract.Requires(sessionStatePersistenceProvider != null);
            Contract.Requires(topicNameRouter != null);
            Contract.Requires(!mqttTransportSettings.HasWill || willMessageProvider != null);

            this.deviceId = deviceId;
            this.iotHubHostName = iotHubHostName;
            this.password = password;
            this.mqttTransportSettings = mqttTransportSettings;
            this.sessionStatePersistenceProvider = sessionStatePersistenceProvider;
            this.topicNameRouter = topicNameRouter;
            this.willMessageProvider = willMessageProvider;
            this.connectCallback = connectCallback;
            this.disconnectCallback = disconnectCallback;
            this.onMessageReceived = onMessageReceived;
            this.maxSupportedQos = QualityOfService.AtLeastOnce;
            this.keepAliveTimeout = this.mqttTransportSettings.KeepAliveInSeconds > 0 ? TimeSpan.FromSeconds(this.mqttTransportSettings.KeepAliveInSeconds) : (TimeSpan?)null;
            this.pingRequestTimeout = this.mqttTransportSettings.KeepAliveInSeconds > 0 ? TimeSpan.FromSeconds(this.mqttTransportSettings.KeepAliveInSeconds / 2d) : TimeSpan.MaxValue;

            this.deviceBoundPublishProcessor = new SimpleWorkQueue<PublishPacket>(this.AcceptMessageAsync);
            this.deviceBoundPubAckProcessor = new OrderedTwoPhaseWorkQueue<int, PublishPacket>(this.AcceptMessageAsync, p => p.PacketId, this.SendAckAsync);

            this.serviceBoundPublishProcessor = new SimpleWorkQueue<PublishWorkItem>(this.SendMessageToServerAsync);
            this.serviceBoundPubAckProcessor = new OrderedTwoPhaseWorkQueue<int, PublishWorkItem>(this.SendMessageToServerAsync, p => p.Packet.PacketId, this.ProcessAckAsync);

            this.sessionContext = new Dictionary<string, string>
            {
                {"DeviceId", this.deviceId}
            };
        }

        QualityOfService ReceivinQualityOfService
        {
            get { return this.mqttTransportSettings.ReceivingQoS > this.maxSupportedQos ? this.maxSupportedQos : this.mqttTransportSettings.ReceivingQoS; }
        }

        QualityOfService PublishToServerQualityOfService
        {
            get { return this.mqttTransportSettings.PublishToServerQoS > this.maxSupportedQos ? this.maxSupportedQos : this.mqttTransportSettings.PublishToServerQoS; }
        }

        //Delivery
        Task AcceptMessageAsync(IChannelHandlerContext context, PublishPacket publish)
        {
            Message message;
            try
            {
                using (Stream bodyStream = publish.Payload.IsReadable() ? new ReadOnlyByteBufferStream(publish.Payload, true) : null)
                {
                    message = new Message(bodyStream);

                    Util.PopulateMessagePropertiesFromPacket(message, publish);
                }
            }
            catch (Exception ex)
            {
                this.onMessageReceived(new MqttMessageReceivedResult { Exception = ex });
                return TaskConstants.Completed;
            }
            this.onMessageReceived(new MqttMessageReceivedResult { Message = message });
            return TaskConstants.Completed;
        }

        Task SendAckAsync(IChannelHandlerContext context, PublishPacket publish)
        {
            this.ResumeReadingIfNecessary(context);
            return Util.WriteMessageAsync(context, PubAckPacket.InResponseTo(publish), ShutdownOnWriteErrorHandler);
        }

        //Sending
        async Task SendMessageToServerAsync(IChannelHandlerContext context, PublishWorkItem publish)
        {
            try
            {
                await Util.WriteMessageAsync(context, publish.Packet, ShutdownOnWriteErrorHandler);
                if (publish.Packet.QualityOfService == QualityOfService.AtMostOnce)
                {
                    publish.Completion.Complete();
                }
            }
            catch (Exception ex)
            {
                publish.Completion.SetException(ex);
            }
        }

        Task ProcessAckAsync(IChannelHandlerContext context, PublishWorkItem publish)
        {
            publish.Completion.Complete();
            return TaskConstants.Completed;
        }

        #region IChannelHandler overrides

        public override void ChannelActive(IChannelHandlerContext context)
        {
            this.stateFlags = StateFlags.NotConnected;

            this.Connect(context);

            base.ChannelActive(context);
        }

        public override Task WriteAsync(IChannelHandlerContext context, object data)
        {
            string packetIdString = data as string;
            if (packetIdString != null)
            {
                return this.SendAckAsync(context, packetIdString);
            }
            
            var message = data as Message;
            if (message != null)
            {
                return this.SendMessageAsync(context, message);
            }

            var packet = data as Packet;
            if (packet != null)
            {
                return Util.WriteMessageAsync(context, packet, ShutdownOnWriteErrorHandler);
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

            if (this.IsInState(StateFlags.Connected) || (this.IsInState(StateFlags.Connecting) && packet.PacketType == PacketType.CONNACK))
            {
                this.ProcessMessage(context, packet);
            }
            else
            {
                // we did not start processing CONNACK yet which means we haven't received it yet but the packet of different type has arrived.
                ShutdownOnError(context);
            }
        }

        public override void ChannelReadComplete(IChannelHandlerContext context)
        {
            base.ChannelReadComplete(context);
            if (this.InboundBacklogSize < this.mqttTransportSettings.MaxPendingInboundMessages)
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

        async void Connect(IChannelHandlerContext context)
        {
            var connectPacket = new ConnectPacket
            {
                ClientId = this.deviceId,
                HasUsername = true,
                Username = this.iotHubHostName + "/" + this.deviceId,
                HasPassword = true,
                Password = this.password,
                KeepAliveInSeconds = this.mqttTransportSettings.KeepAliveInSeconds,
                CleanSession = this.mqttTransportSettings.CleanSession,
                HasWill = this.mqttTransportSettings.HasWill
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

            await Util.WriteMessageAsync(context, connectPacket, ShutdownOnWriteErrorHandler);

            if (this.keepAliveTimeout.HasValue)
            {
                this.lastClientActivityTime = DateTime.UtcNow;
                this.KeepConnectionAlive(context);
            }

            this.CheckConnectTimeout(context);
        }

        async void KeepConnectionAlive(IChannelHandlerContext context)
        {
            try
            {
                await context.Channel.EventLoop.ScheduleAsync(PingServerCallback, context, this.pingRequestTimeout);
            }
            catch (OperationCanceledException)
            {
                
            }
        }

        async void CheckConnectTimeout(IChannelHandlerContext context)
        {
            try
            {
                await context.Channel.EventLoop.ScheduleAsync(CheckConackTimeoutCallback, context, this.mqttTransportSettings.ConnectArrivalTimeout);
            }
            catch (OperationCanceledException)
            {

            }
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

        static async void PingServer(object ctx)
        {
            var context = (IChannelHandlerContext)ctx;
            var self = (MqttIotHubAdapter)context.Handler;
            TimeSpan remainingTime = DateTime.UtcNow - self.lastClientActivityTime - self.pingRequestTimeout;
            if (remainingTime.TotalSeconds > 0)
            {
                await Util.WriteMessageAsync(context, PingReqPacket.Instance, ShutdownOnWriteErrorHandler);
            }

            await context.Channel.EventLoop.ScheduleAsync(PingServerCallback, context, self.pingRequestTimeout);
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
                    this.ProcessConnectAck(context, (ConnAckPacket)packet);
                    break;
                case PacketType.SUBACK:
                    this.ProcessSubscribeAck(context, (SubAckPacket)packet);
                    break;
                case PacketType.PUBLISH:
                    this.ProcessPublish(context, (PublishPacket)packet);
                    break;
                case PacketType.PUBACK:
                    this.serviceBoundPubAckProcessor.CompleteWorkAsync(context, ((PubAckPacket)packet).PacketId);
                    break;
                case PacketType.UNSUBACK:
                case PacketType.PINGRESP:
                    break;
                default:
                    ShutdownOnError(context);
                    break;
            }
        }

        async void ProcessConnectAck(IChannelHandlerContext context, ConnAckPacket packet)
        {
            Exception exception = null;
            if (packet.ReturnCode != ConnectReturnCode.Accepted)
            {
                string reason = "CONNECT failed: " + packet.ReturnCode;
                var iotHubException = new IotHubException(reason);
                ShutdownOnError(context, "CONNECT", iotHubException);
                return;
            }

            if (!this.IsInState(StateFlags.Connecting))
            {
                string reason = "CONNECT has been received, however a session has already been establish. Only one CONNECT?CONACK pair is expected per session.";
                var iotHubException = new IotHubException(reason);
                ShutdownOnError(context, "CONNECT", iotHubException);
                return;
            }

            try
            {
                await this.EstablishSessionStateAsync(this.deviceId, this.mqttTransportSettings.CleanSession, packet.SessionPresent);

                this.stateFlags |= StateFlags.Connected;
                this.ResumeReadingIfNecessary(context);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            if (exception != null)
            {
                ShutdownOnError(context, "CONNECT", exception);
                this.connectCallback(new MqttActionResult {Exception = exception});
                return;
            }
            this.connectCallback(new MqttActionResult());

            string topicFilter = TelemetryTopicFilterFormat.FormatInvariant(this.deviceId);
            var subscribePacket = new SubscribePacket(GenerateNextPacketId(), new SubscriptionRequest(topicFilter, this.ReceivinQualityOfService));

            await Util.WriteMessageAsync(context, subscribePacket, ShutdownOnWriteErrorHandler);
        }

        void ProcessSubscribeAck(IChannelHandlerContext context, SubAckPacket packet)
        {
            this.maxSupportedQos = packet.QualityOfService;
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
            this.ResumeReadingIfNecessary(context);
        }

        async Task SendMessageAsync(IChannelHandlerContext context, Message message)
        {
            string topicName;
            if (!this.topicNameRouter.TryMapRouteToTopicName(RouteSourceType.Notification, new ReadOnlyMergeDictionary<string, string>(this.sessionContext, message.Properties), out topicName))
            {
                throw new IotHubException("Invalid message properties");
            }
            PublishPacket packet = await Util.ComposePublishPacketAsync(context, message, this.mqttTransportSettings.PublishToServerQoS, topicName);
            var publishCompletion = new TaskCompletionSource();
            var workItem = new PublishWorkItem
            {
                Completion = publishCompletion,
                Packet = packet
            };
            switch (this.mqttTransportSettings.PublishToServerQoS)
            {
                case QualityOfService.AtMostOnce:
                    this.serviceBoundPublishProcessor.Post(context, workItem);
                    break;
                case QualityOfService.AtLeastOnce:
                    this.serviceBoundPubAckProcessor.Post(context, workItem);
                    break;
                default:
                    throw new NotSupportedException(string.Format("Unsupported telemetry QoS: '{0}'", this.mqttTransportSettings.PublishToServerQoS));
            }
            await publishCompletion.Task;
        }

        Task SendAckAsync(IChannelHandlerContext context, string packetIdString)
        {
            int packetId;
            if (int.TryParse(packetIdString, out packetId))
            {
                this.deviceBoundPubAckProcessor.CompleteWorkAsync(context, packetId);
            }
            return TaskConstants.Completed;
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
            var actionResult = new MqttActionResult();
            actionResult.Exception = exception;
            var self = (MqttIotHubAdapter)context.Handler;
            self.disconnectCallback(actionResult);
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

                this.CloseIotHubConnection();
                await context.CloseAsync();
            }
            catch
            {
            }
        }

        async void CloseIotHubConnection()
        {
            if (this.IsInState(StateFlags.NotConnected) || this.IsInState(StateFlags.Connecting))
            {
                // closure has happened before IoT Hub connection was established or it was initiated due to disconnect
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
            if (this.InboundBacklogSize == this.mqttTransportSettings.MaxPendingInboundMessages - 1) // we picked up a packet from full queue - now we have more room so order another read
            {
                context.Read();
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
            Closed = 1 << 3
        }

        class PublishWorkItem
        {
            public PublishPacket Packet { get; set; }
            public TaskCompletionSource Completion { get; set; }
        }

        public event Action OnConnected;
    }
}