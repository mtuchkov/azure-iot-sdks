// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Extensions;

    sealed class ConcurrentObjectPool<TKey, TValue> where TValue:class 
    {
        class Entry
        {
            public TKey Key { get; set; }

            public TValue Value { get; set; }

            public int RefCount { get; set; }

            public DateTime LastUpdatedTime { get; set; }
        }


        readonly TimeSpan keepAliveTimeout;
        readonly Func<TValue, Task> disposeCallback;
        readonly IComparer<TKey> keyComparer;
        readonly int poolSize;
        readonly List<List<Entry>> pool = new List<List<Entry>>(); 

        int position;
        readonly object syncRoot = new object();

        public ConcurrentObjectPool(int poolSize, TimeSpan keepAliveTimeout, Func<TValue, Task> disposeCallback)
            : this(poolSize, keepAliveTimeout, disposeCallback, null)
        {
        }

        public ConcurrentObjectPool(int poolSize, TimeSpan keepAliveTimeout, Func<TValue, Task> disposeCallback, IComparer<TKey> keyComparer)
        {
            this.poolSize = poolSize;
            this.keepAliveTimeout = keepAliveTimeout;
            this.disposeCallback = disposeCallback;
            this.keyComparer = keyComparer;
            for (int i = 0; i < poolSize; i++)
            {
                this.pool.Add(new List<Entry>());
            }
        }

        public TValue TakeOrAdd(TKey key, Func<TValue> createCallback)
        {
            lock (this.syncRoot)
            {
                List<Entry> entries = this.pool.FirstOrDefault(x=> x.Any(e => this.AreEqual(key, e.Key)));
                TValue value;

                if (entries == null)
                {
                    entries = this.pool[this.position++ % this.poolSize];
                    value = createCallback();
                    entries.Add(new Entry { Key = key, Value = value, RefCount = 1, LastUpdatedTime = DateTime.Now });
                }
                else
                {
                    Entry entry = entries.First(e => this.AreEqual(key, e.Key));
                    entry.RefCount++;
                    entry.LastUpdatedTime = DateTime.Now;
                    value = entry.Value;
                }
                return value;
            }
        }

        public bool Release(TKey key)
        {
            lock (this.syncRoot)
            {
                foreach (List<Entry> entries in this.pool)
                {
                    Entry entry = entries.FirstOrDefault(e => this.AreEqual(key, e.Key));
                    if (entry == null)
                    {
                        continue;
                    }
                    entry.RefCount--;
                    if (entry.RefCount == 0)
                    {
                        this.ScheduleCleanup(key);
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
                foreach (List<Entry> entries in this.pool)
                {
                    Entry entry = entries.FirstOrDefault(e => this.AreEqual(key, e.Key));
                    if (entry == null)
                    {
                        continue;
                    }
                    entry.RefCount--;
                    if (entry.RefCount == 0 && entry.LastUpdatedTime + this.keepAliveTimeout <= DateTime.Now)
                    {
                        value = entry.Value;
                        entries.Remove(entry);
                    }
                    break;
                }
            }
            if (value != null)
            {
                await this.disposeCallback(value);
            }
        }

        bool AreEqual(TKey key, TKey ekey)
        {
            return this.keyComparer == null ? ekey.Equals(key) : this.keyComparer.Compare(ekey, key) == 0;
        }
    }
}