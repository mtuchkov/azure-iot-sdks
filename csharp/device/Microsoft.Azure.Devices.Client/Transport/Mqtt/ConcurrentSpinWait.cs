// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System;
    using System.Threading;

    /// <summary>
    /// Concurrent version of .NET <see cref="System.Threading.SpinWait"/>
    /// </summary>
    class ConcurrentSpinWait
    {

        // These constants determine the frequency of yields versus spinning. The
        // numbers may seem fairly arbitrary, but were derived with at least some 
        // thought in the design document.  I fully expect they will need to change 
        // over time as we gain more experience with performance.
        internal const int YieldThreshold = 10; // When to switch over to a true yield. 
        internal const int Sleep0EveryHowManyTimes = 5; // After how many yields should we Sleep(0)?
        internal const int Sleep1EveryHowManyTimes = 20; // After how many yields should we Sleep(1)?

        // The number of times we've spun already. 
        int count;
        static readonly bool IsSingleProcessor;

        static ConcurrentSpinWait()
        {
            IsSingleProcessor = Environment.ProcessorCount == 1;
        }

        /// <summary> 
        /// Gets the number of times <see cref="SpinOnce"/> has been called on this instance.
        /// </summary> 
        public int Count => Volatile.Read(ref this.count);

        /// <summary> 
        /// Gets whether the next call to <see cref="SpinOnce"/> will yield the processor, triggering a 
        /// forced context switch.
        /// </summary> 
        /// <value>Whether the next call to <see cref="SpinOnce"/> will yield the processor, triggering a
        /// forced context switch.</value>
        /// <remarks>
        /// On a single-CPU machine, <see cref="SpinOnce"/> always yields the processor. On machines with 
        /// multiple CPUs, <see cref="SpinOnce"/> may yield after an unspecified number of calls.
        /// </remarks> 
        public bool NextSpinWillYield => IsSingleProcessor || Volatile.Read(ref this.count) > YieldThreshold;

        /// <summary>
        /// Performs a single spin. 
        /// </summary>
        /// <remarks> 
        /// This is typically called in a loop, and may change in behavior based on the number of times a 
        /// <see cref="SpinOnce"/> has been called thus far on this instance.
        /// </remarks> 
        public void SpinOnce()
        {
            int currentCount = Volatile.Read(ref this.count);
            if (NextSpinWillYield)
            {
                //
                // We must yield. 
                // 
                // We prefer to call Thread.Yield first, triggering a SwitchToThread. This
                // unfortunately doesn't consider all runnable threads on all OS SKUs. In 
                // some cases, it may only consult the runnable threads whose ideal processor
                // is the one currently executing code. Thus we oc----ionally issue a call to
                // Sleep(0), which considers all runnable threads at equal priority. Even this
                // is insufficient since we may be spin waiting for lower priority threads to 
                // execute; we therefore must call Sleep(1) once in a while too, which considers
                // all runnable threads, regardless of ideal processor and priority, but may 
                // remove the thread from the scheduler's queue for 10+ms, if the system is 
                // configured to use the (default) coarse-grained system timer.
                // 

                long yieldsSoFar = currentCount >= YieldThreshold ? currentCount : currentCount - YieldThreshold;

                if (yieldsSoFar % Sleep1EveryHowManyTimes == Sleep1EveryHowManyTimes - 1)
                {
                    Thread.Sleep(1);
                }
                else if (yieldsSoFar % Sleep0EveryHowManyTimes == Sleep0EveryHowManyTimes - 1)
                {
                    Thread.Sleep(0);
                }
                else
                {
                    Thread.Yield();
                }
            }
            else
            {
                //
                // Otherwise, we will spin. 
                //
                // We do this using the CLR's SpinWait API, which is just a busy loop that
                // issues YIELD/PAUSE instructions to ensure multi-threaded CPUs can react
                // intelligently to avoid starving. (These are NOOPs on other CPUs.) We 
                // choose a number for the loop iteration count such that each successive
                // call spins for longer, to reduce cache contention.  We cap the total 
                // number of spins we are willing to tolerate to reduce delay to the caller, 
                // since we expect most callers will eventually block anyway.
                // 
                Thread.SpinWait(4 << this.count);
            }

            // Finally, increment our spin counter. 

            if (currentCount == int.MaxValue)
            {
                Interlocked.CompareExchange(ref this.count, YieldThreshold, currentCount);
                return;
            }
            Interlocked.Increment(ref this.count);
        }

        public void Reset()
        {
            Interlocked.Exchange(ref this.count, 0);
        }
    }
}