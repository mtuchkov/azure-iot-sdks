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

        const string TelemetryTopicFilterFormat = "devices/{0}/messages/devicebound/#";

        static readonly Action<object> PingServerCallback = PingServer;
        static readonly Action<object> CheckConnAckTimeoutCallback = CheckConnectionTimeout;
        static readonly Func<IChannelHandlerContext, Exception, bool> ShutdownOnWriteErrorHandler = (ctx, ex) => { ShutdownOnError(ctx, ex); return false; };
        static readonly ThreadLocal<Random> ThreadLocalRandom = new ThreadLocal<Random>(() => new Random((int)DateTime.UtcNow.ToFileTimeUtc()));

        readonly MqttTransportSettings mqttTransportSettings;
        readonly IWillMessageProvider willMessageProvider;
        readonly Action<MqttActionResult> connectCallback;
        readonly Action<MqttActionResult> disconnectCallback;
        readonly Action<MqttMessageReceivedResult> onMessageReceived;
        readonly ISessionStatePersistenceProvider sessionStatePersistenceProvider;
        
        readonly string deviceId;
        readonly string iotHubHostName;
        readonly string password;
        readonly TimeSpan pingRequestTimeout;
        readonly Dictionary<string, string> sessionContext;
        readonly QualityOfService maxSupportedQos;

        readonly SimpleWorkQueue<PublishWorkItem> serviceBoundOneWayProcessor;
        readonly OrderedTwoPhaseWorkQueue<int, PublishWorkItem> serviceBoundTwoWayProcessor;
        
        readonly SimpleWorkQueue<PublishPacket> deviceBoundOneWayProcessor;
        readonly OrderedTwoPhaseWorkQueue<int, PublishPacket> deviceBoundTwoWayProcessor;
        
        StateFlags stateFlags;
        QualityOfService maxReceivingSupportedQos;
        DateTime lastChannelActivityTime;

        int InboundBacklogSize => this.deviceBoundOneWayProcessor.BacklogSize + this.deviceBoundTwoWayProcessor.BacklogSize;

        public MqttIotHubAdapter(
            string deviceId, 
            string iotHubHostName, 
            string password, 
            MqttTransportSettings mqttTransportSettings, 
            ISessionStatePersistenceProvider sessionStatePersistenceProvider, 
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
            Contract.Requires(!mqttTransportSettings.HasWill || willMessageProvider != null);

            this.deviceId = deviceId;
            this.iotHubHostName = iotHubHostName;
            this.password = password;
            this.mqttTransportSettings = mqttTransportSettings;
            this.sessionStatePersistenceProvider = sessionStatePersistenceProvider;
            this.willMessageProvider = willMessageProvider;
            this.connectCallback = connectCallback;
            this.disconnectCallback = disconnectCallback;
            this.onMessageReceived = onMessageReceived;
            this.maxSupportedQos = QualityOfService.AtLeastOnce;
            this.pingRequestTimeout = this.mqttTransportSettings.KeepAliveInSeconds > 0 ? TimeSpan.FromSeconds(this.mqttTransportSettings.KeepAliveInSeconds / 2d) : TimeSpan.MaxValue;

            this.deviceBoundOneWayProcessor = new SimpleWorkQueue<PublishPacket>(this.AcceptMessageAsync);
            this.deviceBoundTwoWayProcessor = new OrderedTwoPhaseWorkQueue<int, PublishPacket>(this.AcceptMessageAsync, p => p.PacketId, this.SendAckAsync);

            this.serviceBoundOneWayProcessor = new SimpleWorkQueue<PublishWorkItem>(this.SendMessageToServerAsync);
            this.serviceBoundTwoWayProcessor = new OrderedTwoPhaseWorkQueue<int, PublishWorkItem>(this.SendMessageToServerAsync, p => p.Packet.PacketId, this.ProcessAckAsync);

            this.sessionContext = new Dictionary<string, string>
            {
                {"DeviceId", this.deviceId}
            };
        }

        QualityOfService ReceiveQualityOfService
        {
            get { return this.mqttTransportSettings.ReceivingQoS > this.maxReceivingSupportedQos ? this.maxReceivingSupportedQos : this.mqttTransportSettings.ReceivingQoS; }
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

        public override async Task WriteAsync(IChannelHandlerContext context, object data)
        {
            var message = data as Message;
            if (message != null)
            {
                await this.SendMessageAsync(context, message);
                return;
            }

            string packetIdString = data as string;
            if (packetIdString != null)
            {
                await this.AcknoledgeAsync(context, packetIdString);
                return;
            }

            if (data is Packet)
            {
                await Util.WriteMessageAsync(context, data, ShutdownOnWriteErrorHandler);
                return;
            }

            throw new InvalidOperationException($"Unexpected data type: '{data.GetType().Name}'");
        }

        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            var packet = message as Packet;
            if (packet == null)
            {
                return;
            }

            this.lastChannelActivityTime = DateTime.UtcNow; // notice last client activity - used in handling disconnects on keep-alive timeout

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
                ShutdownOnError(context, handshakeCompletionEvent.Exception);
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
                string willtopicName = "devices/" + this.willMessageProvider.Message.Properties["DeviceId"] + "/messages/events/";
                connectPacket.WillTopicName = willtopicName;
            }
            this.stateFlags = StateFlags.Connecting;

            await Util.WriteMessageAsync(context, connectPacket, ShutdownOnWriteErrorHandler);
            this.lastChannelActivityTime = DateTime.UtcNow;
            this.KeepConnectionAlive(context);

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
                await context.Channel.EventLoop.ScheduleAsync(CheckConnAckTimeoutCallback, context, this.mqttTransportSettings.ConnectArrivalTimeout);
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
                ShutdownOnError(context, new TimeoutException("Connection hasn't been established in time."));
            }
        }

        static async void PingServer(object ctx)
        {
            var context = (IChannelHandlerContext)ctx;
            var self = (MqttIotHubAdapter)context.Handler;
            if (!self.IsInState(StateFlags.Connected))
            {
                return;
            }
            TimeSpan remainingTime = DateTime.UtcNow - self.lastChannelActivityTime - self.pingRequestTimeout;
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
                    this.maxReceivingSupportedQos = ((SubAckPacket)packet).QualityOfService;
                    break;
                case PacketType.PUBLISH:
                    this.ProcessPublish(context, (PublishPacket)packet);
                    break;
                case PacketType.PUBACK:
                    this.serviceBoundTwoWayProcessor.CompleteWorkAsync(context, ((PubAckPacket)packet).PacketId);
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
                ShutdownOnError(context, iotHubException);
                return;
            }

            if (!this.IsInState(StateFlags.Connecting))
            {
                string reason = "CONNECT has been received, however a session has already been establish. Only one CONNECT?CONACK pair is expected per session.";
                var iotHubException = new IotHubException(reason);
                ShutdownOnError(context, iotHubException);
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
                ShutdownOnError(context, exception);
                this.connectCallback(new MqttActionResult {Exception = exception});
                return;
            }
            this.connectCallback(new MqttActionResult());

            string topicFilter = TelemetryTopicFilterFormat.FormatInvariant(this.deviceId);
            var subscribePacket = new SubscribePacket(GenerateNextPacketId(), new SubscriptionRequest(topicFilter, this.ReceiveQualityOfService));

            await Util.WriteMessageAsync(context, subscribePacket, ShutdownOnWriteErrorHandler);
        }

        void ProcessPublish(IChannelHandlerContext context, PublishPacket packet)
        {
            switch (packet.QualityOfService)
            {
                case QualityOfService.AtMostOnce:
                    this.deviceBoundOneWayProcessor.Post(context, packet);
                    break;
                case QualityOfService.AtLeastOnce:
                    this.deviceBoundTwoWayProcessor.Post(context, packet);
                    break;
                default:
                    throw new NotSupportedException(string.Format("Unexpected QoS: '{0}'", packet.QualityOfService));
            }
            this.ResumeReadingIfNecessary(context);
        }

        async Task SendMessageAsync(IChannelHandlerContext context, Message message)
        {
            string topicName = "devices/" + ((IDictionary<string, string>)new ReadOnlyMergeDictionary<string, string>(this.sessionContext, message.Properties))["DeviceId"] + "/messages/events/";

            QualityOfService qos = this.PublishToServerQualityOfService;

            PublishPacket packet = await Util.ComposePublishPacketAsync(context, message, qos, topicName);
            var publishCompletion = new TaskCompletionSource();
            var workItem = new PublishWorkItem
            {
                Completion = publishCompletion,
                Packet = packet
            };
            switch (qos)
            {
                case QualityOfService.AtMostOnce:
                    this.serviceBoundOneWayProcessor.Post(context, workItem);
                    break;
                case QualityOfService.AtLeastOnce:
                    this.serviceBoundTwoWayProcessor.Post(context, workItem);
                    break;
                default:
                    throw new NotSupportedException(string.Format("Unsupported telemetry QoS: '{0}'", qos));
            }
            await publishCompletion.Task;
        }

        Task AcknoledgeAsync(IChannelHandlerContext context, string packetIdString)
        {
            int packetId;
            if (int.TryParse(packetIdString, out packetId))
            {
                return this.deviceBoundTwoWayProcessor.CompleteWorkAsync(context, packetId);
            }
            return TaskConstants.Completed;
        }

        //STOP
        static void ShutdownOnError(IChannelHandlerContext context, Exception exception)
        {
            ShutdownOnError(context);
            var actionResult = new MqttActionResult
            {
                Exception = exception
            };
            var self = (MqttIotHubAdapter)context.Handler;
            self.disconnectCallback(actionResult);
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
                // ignored
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
                this.serviceBoundOneWayProcessor.Complete();
                this.deviceBoundOneWayProcessor.Complete();
                this.serviceBoundTwoWayProcessor.Complete();
                this.deviceBoundTwoWayProcessor.Complete();
                await Task.WhenAll(
                    this.serviceBoundOneWayProcessor.Completion, 
                    this.serviceBoundTwoWayProcessor.Completion,
                    this.deviceBoundOneWayProcessor.Completion,
                    this.deviceBoundTwoWayProcessor.Completion);
            }
            catch
            {
                try
                {
                    this.serviceBoundOneWayProcessor.Abort();
                    this.deviceBoundOneWayProcessor.Abort();
                    this.serviceBoundTwoWayProcessor.Abort();
                    this.deviceBoundTwoWayProcessor.Abort();
                }
                catch
                {
                    // ignored on closing
                }
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
                    this.sessionStatePersistenceProvider.Create(true);
                }
                else
                {
                }
            }
            else
            {
                this.sessionStatePersistenceProvider.Create(true);
            }
        }

        static int GenerateNextPacketId()
        {
            return ThreadLocalRandom.Value.Next(1, ushort.MaxValue);
        }
        #endregion
    }
}