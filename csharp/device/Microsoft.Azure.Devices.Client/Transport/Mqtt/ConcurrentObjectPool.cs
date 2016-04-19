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
            readonly IEqualityComparer<TKey> comparer;

            public TKey Key { get; }

            public int RefCount { get; set; }

            public Entry(TKey key, int refCount, IEqualityComparer<TKey> comparer)
            {
                this.comparer = comparer;
                this.Key = key;
                this.RefCount = refCount;
            }

            public override bool Equals(object obj)
            {
                var other = obj as Entry;
                if (other == null)
                {
                    return false;
                }
                IEqualityComparer<TKey> equalityComparer = this.comparer ?? EqualityComparer<TKey>.Default;
                return equalityComparer.Equals(this.Key, other.Key);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    IEqualityComparer<TKey> equalityComparer = this.comparer ?? EqualityComparer<TKey>.Default;
                    return equalityComparer.GetHashCode(this.Key) * 397;
                }
            }
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
        readonly Slot[] slots;

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
            this.slots = new Slot[poolSize];
            for (int i = 0; i < poolSize; i++)
            {
                this.slots[i] = new Slot
                {
                    Entries = new List<Entry>()
                };
            }
        }

        public TValue TakeOrAdd(TKey key)
        {
            lock (this.syncRoot)
            {
                Slot slot = this.slots.FirstOrDefault(s => s.Entries.Any(x => this.IsKeyPresent(x, key)));
                TValue value;

                if (slot == null)
                {
                    slot = this.slots[this.position++ % this.poolSize];
                    if (slot.Value == null)
                    {
                        slot.Value = this.objectFactory();
                    }
                    value = slot.Value;
                    slot.Entries.Add(new Entry(key, 1, this.keyComparer));
                    slot.LastUpdatedTime = DateTime.UtcNow;
                }
                else
                {
                    Entry entry = slot.Entries.First(x => this.IsKeyPresent(x, key));
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
                for (int i = 0; i < this.slots.Length; i++)
                {
                    Slot slot = this.slots[i];
                    Entry entry = slot.Entries.FirstOrDefault(x => this.IsKeyPresent(x, key));
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
                            this.ScheduleCleanup(slot, i);
                        }
                    }
                    break;
                }
            }
            return true;
        }

        async void ScheduleCleanup(Slot key, int index)
        {
            try
            {
                await Task.Delay(this.keepAliveTimeout);
                await this.DisposeAsync(key, index);
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
            }
        }

        async Task DisposeAsync(Slot slot, int index)
        {
            TValue value = null;
            lock (this.syncRoot)
            {
                if (this.slots[index] != null && this.slots[index] == slot && slot.Entries.Count == 0 && slot.LastUpdatedTime + this.keepAliveTimeout <= DateTime.UtcNow)
                {
                    value = slot.Value;
                    this.slots[index] = null;
                }
            }
            if (value != null)
            {
                await this.disposeCallback(value);
            }
        }

        bool IsKeyPresent(Entry x, TKey key)
        {
            return this.keyComparer?.Equals(key, x.Key) ?? x.Key.Equals(key);
        }
    }
}