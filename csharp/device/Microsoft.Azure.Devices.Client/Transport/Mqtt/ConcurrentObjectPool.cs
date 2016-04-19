// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Extensions;

    sealed class ConcurrentObjectPool<TKey, TValue> where TValue : class
    {
        class Entry
        {
            public HashSet<TKey> Keys { get; set; }

            public int RefCount { get; set; }
        }

        class Slot
        {
            public List<Entry> Entries { get; set; }

            public DateTime LastUpdatedTime { get; set; }

            public TValue Value { get; set; }
        }
        
        readonly TimeSpan keepAliveTimeout;
        readonly Func<TValue, Task> disposeCallback;
        readonly IEqualityComparer<TKey> keyComparer;
        readonly int poolSize;
        readonly Func<TValue> objectFactory;
        readonly Slot[] pool;

        int position;
        readonly object syncRoot = new object();

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
            this.pool = new Slot[poolSize];
            for (int i = 0; i < poolSize; i++)
            {
                this.pool[i] = new Slot
                {
                    Entries = new List<Entry>()
                };
            }
        }

        public TValue TakeOrAdd(TKey key)
        {
            lock (this.syncRoot)
            {
                Slot slot = this.pool.FirstOrDefault(s => s.Entries.Any(e => e.Keys.Contains(key)));
                TValue value;

                if (slot == null)
                {
                    slot = this.pool[this.position++ % this.poolSize];
                    if (slot.Value == null)
                    {
                        slot.Value = this.objectFactory();
                    }
                    value = slot.Value;
                    slot.Entries.Add(new Entry { RefCount = 1, Keys = new HashSet<TKey> { key } });
                    slot.LastUpdatedTime = DateTime.Now;
                }
                else
                {
                    Entry entry = slot.Entries.First(e => e.Keys.Contains(key));
                    entry.RefCount++;
                    slot.LastUpdatedTime = DateTime.Now;
                    value = slot.Value;
                }
                return value;
            }
        }

        public bool Release(TKey key)
        {
            lock (this.syncRoot)
            {
                foreach (Slot slot in this.pool)
                {
                    Entry entry = slot.Entries.FirstOrDefault(e => e.Keys.Contains(key));
                    if (entry == null)
                    {
                        continue;
                    }
                    entry.RefCount--;
                    if (entry.RefCount == 0)
                    {
                        slot.Entries.Remove(entry);
                        if (slot.Entries.Count == 0)
                        {
                            this.ScheduleCleanup(key);
                        }
                    }
                    break;
                }
            }
            return true;
        }

        async void ScheduleCleanup(TKey key)
        {
            try
            {
                await Task.Delay(this.keepAliveTimeout);
                await this.RemoveAsync(key);
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
            }
        }

        async Task RemoveAsync(TKey key)
        {
            TValue value = null;
            lock (this.syncRoot)
            {
                foreach (Slot slot in this.pool)
                {
                    Entry entry = slot.Entries.FirstOrDefault(x => this.keyComparer == null ? x.Keys.Contains(key) : x.Keys.Contains(key, this.keyComparer));
                    if (entry != null)
                    {
                        entry.RefCount--;
                        if (entry.RefCount == 0)
                        {
                            slot.Entries.Remove(entry);
                        }
                    }
                    if (slot.Entries.Count == 0 && slot.LastUpdatedTime + this.keepAliveTimeout <= DateTime.Now)
                    {
                        value = slot.Value;
                    }
                    break;
                }
            }
            if (value != null)
            {
                await this.disposeCallback(value);
            }
        }
    }
}