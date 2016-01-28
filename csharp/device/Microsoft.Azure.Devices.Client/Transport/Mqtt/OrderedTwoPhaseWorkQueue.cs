// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DotNetty.Transport.Channels;

    class OrderedTwoPhaseWorkQueue<TWorkId, TWork> : SimpleWorkQueue<TWork> where TWorkId:IEquatable<TWorkId>
    {
        class IncompleteWorkItem
        {
            public IncompleteWorkItem(TWorkId id, TWork workItem)
            {
                this.WorkItem = workItem;
                this.Id = id;
            }

            public TWork WorkItem { get; set; }

            public TWorkId Id { get; set; }
        }
        readonly Func<TWork, TWorkId> getWorkId;
        readonly Func<IChannelHandlerContext, TWork, Task> completeWork;
        readonly Queue<IncompleteWorkItem> incompleteQueue = new Queue<IncompleteWorkItem>();

        public OrderedTwoPhaseWorkQueue(Func<IChannelHandlerContext, TWork, Task> worker, Func<TWork, TWorkId> getWorkId, Func<IChannelHandlerContext, TWork, Task> completeWork)
            : base(worker)
        {
            this.getWorkId = getWorkId;
            this.completeWork = completeWork;
        }

        public Task CompleteWorkAsync(IChannelHandlerContext context, TWorkId workId)
        {
            IncompleteWorkItem incompleteWorkItem = this.incompleteQueue.Dequeue();
            if (incompleteWorkItem.Id .Equals(workId))
            {
                return this.completeWork(context, incompleteWorkItem.WorkItem);
            }
            throw new InvalidOperationException(string.Format("Work must be complete in the same order as it was started. Expected work id: '{0}', actual work id: '{1}'", incompleteWorkItem.Id, workId));
        }

        protected override async Task DoWorkAsync(IChannelHandlerContext context, TWork work)
        {
            await base.DoWorkAsync(context, work);
            this.incompleteQueue.Enqueue(new IncompleteWorkItem(this.getWorkId(work), work));
        }
    }
}