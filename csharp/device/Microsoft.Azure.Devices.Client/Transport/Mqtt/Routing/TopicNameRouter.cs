// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt.Routing
{
    using System.Collections.Generic;

    sealed class TopicNameRouter : ITopicNameRouter
    {
        public bool TryMapRouteToTopicName(MqttIotHubAdapter.RouteSourceType routeType, IDictionary<string, string> context, out string topicName)
        {
            topicName = "devices/" + context["DeviceId"] + "/messages/events/";
            return true;
        }

        /// <summary>
        /// Service-bound mapping
        /// </summary>
        /// <param name="topicName"></param>
        /// <param name="routeType"></param>
        /// <param name="contextOutput"></param>
        /// <returns></returns>
        public bool TryMapTopicNameToRoute(string topicName, out RouteDestinationType routeType, IDictionary<string, string> contextOutput)
        {
            contextOutput["DeviceId"] = topicName.Substring(8, 32);
            routeType = RouteDestinationType.Telemetry;
            return false;
        }
    }
}