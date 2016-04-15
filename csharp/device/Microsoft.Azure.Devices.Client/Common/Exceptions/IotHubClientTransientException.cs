// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    class IotHubClientTransientException : IotHubClientException
    {
        public IotHubClientTransientException(string message)
            : base(message)
        {
        }

        public IotHubClientTransientException(string message, string trackingId)
            : base(message, trackingId)
        {
        }

        public IotHubClientTransientException(string message, bool isTransient, string trackingId)
            : base(message, isTransient, trackingId)
        {
        }

        public IotHubClientTransientException(string message, bool isTransient)
            : base(message, isTransient)
        {
        }

        public IotHubClientTransientException(Exception innerException)
            : base(innerException)
        {
        }

        public IotHubClientTransientException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public IotHubClientTransientException(string message, Exception innerException, bool isTransient)
            : base(message, innerException, isTransient)
        {
        }

        public IotHubClientTransientException(string message, Exception innerException, bool isTransient, string trackingId)
            : base(message, innerException, isTransient, trackingId)
        {
        }

        public IotHubClientTransientException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}