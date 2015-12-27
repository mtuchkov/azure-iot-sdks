// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt.Routing
{
    using System;

    class OutboundRouteDefinition
    {
        public MqttIotHubAdapter.RouteSourceType Type { get; private set; }

        public string Template { get; private set; }

        OutboundRouteDefinition(MqttIotHubAdapter.RouteSourceType type, string template)
        {
            this.Type = type;
            this.Template = template;
        }

        public static OutboundRouteDefinition Create(MqttIotHubAdapter.RouteSourceType type, string template)
        {
            var outboundRouteDefinition = new OutboundRouteDefinition(type, template);
            outboundRouteDefinition.Validate();
            return outboundRouteDefinition;
        }

        void Validate()
        {
            switch (this.Type)
            {
                case MqttIotHubAdapter.RouteSourceType.Unknown:
                    throw new InvalidOperationException("Route type cannot be `Unknown`.");
                case MqttIotHubAdapter.RouteSourceType.Notification:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}