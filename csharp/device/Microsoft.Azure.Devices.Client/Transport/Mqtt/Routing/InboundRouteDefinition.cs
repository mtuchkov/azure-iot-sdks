// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt.Routing
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    class InboundRouteDefinition
    {
        public RouteDestinationType Type { get; private set; }

        public IEnumerable<string> Templates { get; private set; }

        InboundRouteDefinition(IEnumerable<string> templates, RouteDestinationType type)
        {
            this.Type = type;
            this.Templates = templates.ToArray();
        }

        public static InboundRouteDefinition Create(IEnumerable<string> templates, RouteDestinationType type)
        {
            var inboundRouteDefinition = new InboundRouteDefinition(templates, type);
            inboundRouteDefinition.Validate();
            return inboundRouteDefinition;
        }

        void Validate()
        {
            if (!this.Templates.Any())
            {
                throw new InvalidOperationException("Route must have at least one template specified.");
            }
            switch (this.Type)
            {
                case RouteDestinationType.Unknown:
                    throw new InvalidOperationException("Route type cannot be `Unknown`.");
                case RouteDestinationType.Telemetry:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}