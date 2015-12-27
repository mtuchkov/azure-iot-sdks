// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt.Store
{
    using System.Collections.Generic;

    public sealed class SessionState : ISessionState
    {
        public SessionState(bool transient)
        {
            this.IsTransient = transient;
            this.Subscriptions = new List<Subscription>();
        }

        public ISessionState Copy()
        {
            var sessionState = new SessionState(this.IsTransient);
            sessionState.Subscriptions.AddRange(this.Subscriptions);
            return sessionState;
        }

        public List<Subscription> Subscriptions { get; private set; }

        public bool IsTransient { get; private set; }
    }
}