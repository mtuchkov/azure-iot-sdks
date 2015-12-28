// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics.Contracts;
using DotNetty.Codecs.Mqtt.Packets;

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt.Store
{
    public class Subscription
    {
        public Subscription(string topicFilter, QualityOfService qualityOfService)
            : this(DateTime.UtcNow, topicFilter, qualityOfService)
        {
        }

        Subscription(DateTime creationTime, string topicFilter, QualityOfService qualityOfService)
        {
            this.CreationTime = creationTime;
            this.TopicFilter = topicFilter;
            this.QualityOfService = qualityOfService;
        }

        public DateTime CreationTime { get; private set; }

        public string TopicFilter { get; private set; }

        public QualityOfService QualityOfService { get; private set; }

        [Pure]
        public Subscription CreateUpdated(QualityOfService qos)
        {
            return new Subscription(this.CreationTime, this.TopicFilter, qos);
        }
    }
}