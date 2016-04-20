// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Extensions;

    sealed class ConcurrentObjectPool<TKey, TValue> where TValue : class
    {
        class Entry
        {
            public TKey Key { get; }

            public int RefCount { get; set; }

            public Entry(TKey key, int refCount)
            {
                this.Key = key;
                this.RefCount = refCount;
            }
        }

        class Slot
        {
            public Dictionary<TKey, Entry> Entries { get; set; }

            public DateTime LastUpdatedTime { get; set; }

            public TValue Value { get; set; }
        }
        
        readonly TimeSpan keepAliveTimeout;
        readonly Func<TValue, Task> disposeCallback;
        readonly IEqualityComparer<TKey> keyComparer;
        readonly int poolSize;
        readonly Func<TValue> objectFactory;
        readonly Slot[] slots;

        int position;
        readonly object syncRoot = new object();
        Timer cleanupTimer;

        public ConcurrentObjectPool(int poolSize, Func<TValue> objectFactory, TimeSpan keepAliveTimeout, Func<TValue, Task> disposeCallback)
            : this(poolSize, objectFactory, keepAliveTimeout, disposeCallback, null)
        {
        }

        public ConcurrentObjectPool(int poolSize, Func<TValue> objectFactory, TimeSpan keepAliveTimeout, Func<TValue, Task> disposeCallback, IEqualityComparer<TKey> keyComparer)
        {
            this.poolSize = poolSize;
            this.objectFactory = objectFactory;
            this.keepAliveTimeout = keepAliveTimeout;
            this.disposeCallback = disposeCallback;
            this.keyComparer = keyComparer;
            this.slots = new Slot[poolSize];
            for (int i = 0; i < poolSize; i++)
            {
                this.slots[i] = new Slot
                {
                    Entries = new Dictionary<TKey, Entry>(this.keyComparer)
                };
            }
            this.cleanupTimer = new Timer(this.Cleanup, null, this.keepAliveTimeout, this.keepAliveTimeout);
        }

        public TValue TakeOrAdd(TKey key)
        {
            lock (this.syncRoot)
            {
                Slot slot = this.slots.FirstOrDefault(s => s.Entries.ContainsKey(key));
                TValue value;

                if (slot == null)
                {
                    slot = this.slots[this.position++ % this.poolSize];
                    if (slot.Value == null)
                    {
                        slot.Value = this.objectFactory();
                    }
                    value = slot.Value;
                    slot.Entries.Add(key, new Entry(key, 1));
                    slot.LastUpdatedTime = DateTime.UtcNow;
                }
                else
                {
                    Entry entry = slot.Entries[key];
                    entry.RefCount++;
                    slot.LastUpdatedTime = DateTime.UtcNow;
                    value = slot.Value;
                }
                return value;
            }
        }

        public bool Release(TKey key)
        {
            lock (this.syncRoot)
            {
                foreach (Slot slot in this.slots)
                {
                    Entry entry; 
                    if (!slot.Entries.TryGetValue(key, out entry) || entry == null)
                    {
                        continue;
                    }
                    entry.RefCount--;
                    if (entry.RefCount == 0)
                    {
                        slot.Entries.Remove(key);
                    }
                    break;
                }
            }
            return true;
        }

        void Cleanup(object state)
        {
            var objectsToCleanup = new List<TValue>();
            lock (this.syncRoot)
            {
                for (int i = 0; i < poolSize; i++)
                {
                    Slot slot = this.slots[i];
                    if (slot.Entries.Count == 0 && slot.LastUpdatedTime < DateTime.UtcNow - this.keepAliveTimeout)
                    {
                        objectsToCleanup.Add(slot.Value);
                        this.slots[i] = null;
                    }
                }
            }
            foreach (TValue obj in objectsToCleanup)
            {
                this.ScheduleDispose(obj);
            }
        }

        async void ScheduleDispose(TValue obj)
        {
            try
            {
                await this.disposeCallback(obj);
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
            }
        }

        bool IsKeyPresent(Entry x, TKey key)
        {
            return this.keyComparer?.Equals(key, x.Key) ?? x.Key.Equals(key);
        }
    }
}