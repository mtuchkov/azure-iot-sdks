// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport
{
    using System;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Net.Sockets;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Exceptions;

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
                    await base.OpenAsync(explicitOpen);
                }
                catch (Exception exception)
                {
                    if (this.InnerHandler != null)
                    {
                        await base.CloseAsync();
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
                throw new InvalidOperationException("Unable to open transport", lastException);
            }
        }
    }
}