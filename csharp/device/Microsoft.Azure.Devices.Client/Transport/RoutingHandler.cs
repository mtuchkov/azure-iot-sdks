// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport
{
    using System;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Linq;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Exceptions;
    using Microsoft.Azure.Devices.Client.Extensions;

    class RoutingHandler : DefaultDelegatingHandler
    {
        internal delegate DefaultDelegatingHandler TransportHandlerFactory(IotHubConnectionString iotHubConnectionString, ITransportSettings transportSettings);

        readonly TransportHandlerFactory transportHandlerFactory;
        readonly IotHubConnectionString iotHubConnectionString;
        readonly ITransportSettings[] transportSettings;

        public RoutingHandler(TransportHandlerFactory transportHandlerFactory, IotHubConnectionString iotHubConnectionString, ITransportSettings[] transportSettings)
        {
            this.transportHandlerFactory = transportHandlerFactory;
            this.iotHubConnectionString = iotHubConnectionString;
            this.transportSettings = transportSettings;
        }

        public override async Task OpenAsync(bool explicitOpen)
        {
            await this.TryOpenPrioritizedTransportsAsync(explicitOpen);
        }

        async Task TryOpenPrioritizedTransportsAsync(bool explicitOpen)
        {
            Exception lastException = null;
            // Concrete Device Client creation was deferred. Use prioritized list of transports.
            foreach (ITransportSettings transportSetting in this.transportSettings)
            {
                try
                {
                    this.InnerHandler = this.transportHandlerFactory(this.iotHubConnectionString, transportSetting);

                    // Try to open a connection with this transport
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();
                    await base.OpenAsync(explicitOpen);
                    stopwatch.Stop();
                    long elapsedMilliseconds = stopwatch.ElapsedMilliseconds;
                    bool lockTaken = false;
                    try
                    {
                        spinLock.TryEnter(1, ref lockTaken);
                        if (lockTaken)
                        {
                            latencies[(int)elapsedMilliseconds]++;
                        }
                    }
                    finally
                    {
                        if (lockTaken)
                        {
                            spinLock.Exit(false);
                        }
                    }
                }
                catch (Exception exception)
                {
                    try
                    {
                        if (this.InnerHandler != null)
                        {
                            await base.CloseAsync();
                        }
                    }
                    catch (Exception ex) when (!ex.IsFatal())
                    {
                        //ignore close failures    
                    }

                    if (!(exception is IotHubCommunicationException || exception is TimeoutException || exception is SocketException || exception is AggregateException))
                    {
                        throw;
                    }

                    var aggregateException = exception as AggregateException;
                    if (aggregateException != null)
                    {
                        ReadOnlyCollection<Exception> innerExceptions = aggregateException.Flatten().InnerExceptions;
                        if (!innerExceptions.Any(x => x is IotHubCommunicationException || x is SocketException || x is TimeoutException))
                        {
                            throw;
                        }
                    }

                    lastException = exception;

                    // open connection failed. Move to next transport type
                    continue;
                }

                return;
            }

            if (lastException != null)
            {
                throw new IotHubCommunicationException("Unable to open transport", lastException);
            }
        }

        internal static readonly int[] latencies = new int[1000 * 60 * 10];
        internal static SpinLock spinLock = new SpinLock();
        
        public RoutingHandler()
        {
            
        }

        public override async Task SendEventAsync(Message message)
        {
            await base.SendEventAsync(message);
        }
    }
}