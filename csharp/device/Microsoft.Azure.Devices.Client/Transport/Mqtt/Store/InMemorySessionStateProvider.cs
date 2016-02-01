// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt.Store
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client.Common;

    public sealed class InMemorySessionStateProvider : ISessionStatePersistenceProvider
    {
        readonly Dictionary<string, ISessionState> sessionStates = new Dictionary<string, ISessionState>();

        public ISessionState Create(bool transient)
        {
            return new SessionState(transient);
        }

        public Task<ISessionState> GetAsync(string id)
        {
            ISessionState sessionState = this.sessionStates.ContainsKey(id) ? this.sessionStates[id] : null;
            return Task.FromResult(sessionState);
        }

        public Task SetAsync(string id, ISessionState sessionState)
        {
            this.sessionStates[id] = sessionState;
            return TaskConstants.Completed;
        }

        public Task DeleteAsync(string id, ISessionState sessionState)
        {
            var state = sessionState as SessionState;

            if (state == null)
            {
                throw new ArgumentException("Cannot set Session State object that hasn't been acquired from provider.", "sessionState");
            }

            this.sessionStates.Remove(id);

            return TaskConstants.Completed;
        }
    }
}