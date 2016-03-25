// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DotNetty.Transport.Channels;
    using Microsoft.Azure.Devices.Client.Common;

    class EventLoopGroupPool
    {
        static readonly int PoolSize = Environment.ProcessorCount * 2;
        static long counter = -1;
        static readonly object SyncRoot = new object();

        public static readonly EventLoopGroupPool Instance;

        readonly MultithreadEventLoopGroup[] eventLoopGroups = new MultithreadEventLoopGroup[PoolSize];

        static EventLoopGroupPool()
        {
            Instance = new EventLoopGroupPool();
        }

        EventLoopGroupPool()
        {
            
        }

        public IEventLoopGroup GetNext()
        {
            lock (SyncRoot)
            {
                counter++;

                if (counter >= PoolSize)
                {
                    return this.eventLoopGroups[counter % PoolSize];
                }

                if (this.eventLoopGroups[counter] == null)
                {
                    this.eventLoopGroups[counter] = new MultithreadEventLoopGroup(() => new SingleThreadEventLoop("MQTTClientExecutionThread_" + counter, TimeSpan.FromMilliseconds(100)), 1);
                }

                return this.eventLoopGroups[counter % PoolSize];
            }
        }

        public Task DisposeAsync()
        {
            var cleanupTasks = new List<Task>();
            lock (SyncRoot)
            {
                counter--;
                if (counter < 0)
                {
                    for (int i = 1; i < PoolSize; i++)
                    {
                        cleanupTasks.Add(this.eventLoopGroups[i].ShutdownGracefullyAsync());
                        this.eventLoopGroups[i] = null;
                    }
                }
            }
            return cleanupTasks.Count > 0 ? Task.WhenAll(cleanupTasks) : TaskConstants.Completed;
        }
    }
}