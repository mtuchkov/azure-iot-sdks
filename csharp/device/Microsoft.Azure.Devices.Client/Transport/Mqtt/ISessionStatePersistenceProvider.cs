// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System.Threading.Tasks;

    public interface ISessionStatePersistenceProvider
    {
        ISessionState Create(bool transient);

        Task<ISessionState> GetAsync(string id);

        Task SetAsync(string id, ISessionState sessionState);

        Task DeleteAsync(string id, ISessionState sessionState);
    }
}