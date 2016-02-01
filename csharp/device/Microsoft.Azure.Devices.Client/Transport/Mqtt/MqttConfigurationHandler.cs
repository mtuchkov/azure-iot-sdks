// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System.Configuration;
    using System.Xml;

    public class MqttConfigurationHandler : IConfigurationSectionHandler
    {
        public object Create(object parent, object configContext, XmlNode section)
        {
            var configuration = new MqttTransportSettings((XmlElement)section);
            return configuration;
        }
    }
}