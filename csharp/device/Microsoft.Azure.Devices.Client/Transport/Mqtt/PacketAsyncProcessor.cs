// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.ProtocolGateway.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.Threading.Tasks;
    using DotNetty.Codecs.Mqtt.Packets;
    using DotNetty.Common.Concurrency;
    using DotNetty.Common.Utilities;
    using DotNetty.Transport.Channels;

    public sealed class PacketAsyncProcessor<T> : PacketAsyncProcessorBase<T>
    {
        readonly Func<IChannelHandlerContext, T, Task> processFunc;

        public PacketAsyncProcessor(Func<IChannelHandlerContext, T, Task> processFunc)
        {
            this.processFunc = processFunc;
        }

        protected override Task ProcessAsync(IChannelHandlerContext context, T packet)
        {
            return this.processFunc(context, packet);
        }
    }

    public abstract class PacketAsyncProcessorBase<T>
    {
        readonly Queue<T> backlogQueue;
        State state;
        readonly TaskCompletionSource completionSource;

        protected PacketAsyncProcessorBase()
        {
            this.backlogQueue = new Queue<T>();
            this.completionSource = new TaskCompletionSource();
        }

        public Task Completion
        {
            get { return this.completionSource.Task; }
        }

        public int BacklogSize
        {
            get { return this.backlogQueue.Count; }
        }

        public void Post(IChannelHandlerContext context, T packet)
        {
            switch (this.state)
            {
                case State.Idle:
                    this.backlogQueue.Enqueue(packet);
                    this.state = State.Processing;
                    this.StartQueueProcessingAsync(context);
                    break;
                case State.Processing:
                case State.FinalProcessing:
                    this.backlogQueue.Enqueue(packet);
                    break;
                case State.Aborted:
                    ReferenceCountUtil.Release(packet);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Complete()
        {
            switch (this.state)
            {
                case State.Idle:
                    this.completionSource.TryComplete();
                    break;
                case State.Processing:
                    this.state = State.FinalProcessing;
                    break;
                case State.FinalProcessing:
                case State.Aborted:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Abort()
        {
            switch (this.state)
            {
                case State.Idle:
                case State.Processing:
                case State.FinalProcessing:
                    this.state = State.Aborted;

                    Queue<T> queue = this.backlogQueue;
                    while (queue.Count > 0)
                    {
                        T packet = queue.Dequeue();
                        ReferenceCountUtil.Release(packet);
                    }
                    break;
                case State.Aborted:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        async void StartQueueProcessingAsync(IChannelHandlerContext context)
        {
            try
            {
                Queue<T> queue = this.backlogQueue;
                while (queue.Count > 0 && this.state != State.Aborted)
                {
                    T message = queue.Dequeue();
                    await this.ProcessAsync(context, message);
                }

                switch (this.state)
                {
                    case State.Processing:
                        this.state = State.Idle;
                        break;
                    case State.FinalProcessing:
                    case State.Aborted:
                        this.completionSource.TryComplete();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (Exception ex)
            {
                this.Abort();
                this.completionSource.TrySetException(new ChannelMessageProcessingException(ex, context));
            }
        }

        protected abstract Task ProcessAsync(IChannelHandlerContext context, T packet);

        enum State
        {
            Idle,
            Processing,
            FinalProcessing,
            Aborted
        }
    }

    interface ISupportRetransmission
    {
        DateTime SentTime { get; }

        void ResetSentTime();
    }

    interface IPacketReference
    {
        int PacketId { get; }
    }

    sealed class RequestAckPairProcessor<TAckState, TRequest> : PacketAsyncProcessorBase<PacketWithId>
        where TAckState : ISupportRetransmission, IPacketReference
    {
        // ReSharper disable once StaticMemberInGenericType -- generic type is used sparingly
        static readonly Action<object, object> StartRetransmissionIfNeededCallback = StartRetransmissionIfNeeded;

        Queue<TAckState> pendingAckQueue;
        readonly Func<IChannelHandlerContext, TAckState, Task> processAckFunc;
        readonly Action<IChannelHandlerContext, TAckState> triggerRetransmissionAction;
        bool retransmissionCheckScheduled;
        readonly TimeSpan ackTimeout;

        public RequestAckPairProcessor(Func<IChannelHandlerContext, TAckState, Task> processAckFunc,
            Action<IChannelHandlerContext, TAckState> triggerRetransmissionAction, TimeSpan? ackTimeout)
        {
            Contract.Requires(!ackTimeout.HasValue || ackTimeout.Value > TimeSpan.Zero);

            this.processAckFunc = processAckFunc;
            this.triggerRetransmissionAction = triggerRetransmissionAction;
            this.ackTimeout = ackTimeout ?? TimeSpan.Zero;
        }

        public TAckState FirstRequestPendingAck
        {
            get { return this.RequestPendingAckCount == 0 ? default(TAckState) : this.pendingAckQueue.Peek(); }
        }

        public int RequestPendingAckCount
        {
            get { return this.pendingAckQueue == null ? 0 : this.pendingAckQueue.Count; }
        }

        public bool Retransmitting { get; set; }

        Queue<TAckState> PendingAckQueue
        {
            get { return this.pendingAckQueue ?? (this.pendingAckQueue = new Queue<TAckState>(4)); }
        }

        bool AckCanTimeout
        {
            get { return this.ackTimeout > TimeSpan.Zero; }
        }

        public Task SendRequestAsync(IChannelHandlerContext context, TRequest requestMessage, TAckState ackState)
        {
            this.PendingAckQueue.Enqueue(ackState);

            if (this.CheckAndScheduleRetransmission(context))
            {
                // retransmission is underway so the message has to be sent as part of retransmission;
                // we cannot abandon message right now as it would mess up the order so we leave message in the queue without sending it.
                return TaskEx.Completed;
            }

            return Util.WriteMessageAsync(context, requestMessage);
        }

        public async Task RetransmitAsync(IChannelHandlerContext context, TRequest message, TAckState state)
        {
            state.ResetSentTime();
            await context.WriteAndFlushAsync(message);
            this.ScheduleRetransmissionCheck(context, this.ackTimeout);
        }

        public bool ResumeRetransmission(IChannelHandlerContext context)
        {
            if (this.Retransmitting)
            {
                this.triggerRetransmissionAction(context, this.FirstRequestPendingAck);
                return true;
            }

            return false;
        }

        /// <summary>
        /// </summary>
        /// <param name="context"></param>
        /// <returns>true if retransmission is already underway, otherwise false.</returns>
        bool CheckAndScheduleRetransmission(IChannelHandlerContext context)
        {
            if (this.Retransmitting)
            {
                return true;
            }

            if (this.AckCanTimeout)
            {
                // if retransmission is configured, schedule check for timeout when posting first message in queue
                this.ScheduleRetransmissionCheck(context, this.ackTimeout);
            }
            return false;
        }

        void ScheduleRetransmissionCheck(IChannelHandlerContext context, TimeSpan delay)
        {
            Contract.Requires(this.AckCanTimeout);

            if (!this.retransmissionCheckScheduled)
            {
                this.retransmissionCheckScheduled = true;
                context.Channel.EventLoop.ScheduleAsync(StartRetransmissionIfNeededCallback, context, this, delay);
            }
        }

        static void StartRetransmissionIfNeeded(object ctx, object s)
        {
            var context = (IChannelHandlerContext)ctx;
            var self = (RequestAckPairProcessor<TAckState, TRequest>)s;

            self.retransmissionCheckScheduled = false;

            TAckState messageState = self.FirstRequestPendingAck;
            if (messageState != null)
            {
                TimeSpan timeoutLeft = self.ackTimeout - (DateTime.UtcNow - messageState.SentTime);
                if (timeoutLeft.Ticks <= 0)
                {
                    // entering retransmission mode
                    self.Retransmitting = true;
                    self.triggerRetransmissionAction(context, messageState);
                }
                else
                {
                    // rescheduling check for when timeout would happen for current top message pending ack
                    self.ScheduleRetransmissionCheck(context, timeoutLeft);
                }
            }
        }

        bool TryDequeueMessage(PacketWithId packet, out TAckState message)
        {
            TAckState firstRequest = this.FirstRequestPendingAck;
            if (firstRequest == null)
            {
                message = default(TAckState);
                return false;
            }

            if (packet.PacketId != firstRequest.PacketId)
            {
                message = default(TAckState);
                return false;
            }

            TAckState dequeued = this.pendingAckQueue.Dequeue();
            Contract.Assert(ReferenceEquals(dequeued, firstRequest));

            if (this.pendingAckQueue.Count == 0)
            {
                this.Retransmitting = false;
            }

            message = firstRequest;
            return true;
        }

        protected override Task ProcessAsync(IChannelHandlerContext context, PacketWithId packet)
        {
            TAckState message;
            return this.TryDequeueMessage(packet, out message) ? this.processAckFunc(context, message) : TaskEx.Completed;
        }
    }

    public class ChannelMessageProcessingException : Exception
    {
        public ChannelMessageProcessingException(Exception innerException, IChannelHandlerContext context)
            : base(string.Empty, innerException)
        {
            this.Context = context;
        }

        public IChannelHandlerContext Context { get; private set; }
    }
}