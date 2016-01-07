// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    using System.Diagnostics.Contracts;

    class TypeLoader
    {
        public T LoadImplementation<T>()
        {
            Contract.Assert(typeof(T).IsInterface);

            throw new System.NotImplementedException();
        }
    }
}