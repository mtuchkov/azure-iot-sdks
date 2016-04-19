﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using Microsoft.Azure.Devices.Client.Extensions;
    using Microsoft.Azure.Devices.Client.Transport;
#if !WINDOWS_UWP && !PCL
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
#endif

    // C# using aliases cannot name an unbound generic type declaration without supplying type arguments
    // Therefore, define a separate alias for each type argument
#if WINDOWS_UWP
    using AsyncTask = Windows.Foundation.IAsyncAction;
    using AsyncTaskOfMessage = Windows.Foundation.IAsyncOperation<Message>;
#else
    using AsyncTask = System.Threading.Tasks.Task;
    using AsyncTaskOfMessage = System.Threading.Tasks.Task<Message>;
#endif

    public class DeviceStore
    {
        public static readonly ConcurrentDictionary<string, ConcurrentQueue<DeviceClient>> Clients = new ConcurrentDictionary<string, ConcurrentQueue<DeviceClient>>();
    }

    /// <summary>
    /// Transport types supported by DeviceClient - Amqp and HTTP 1.1
    /// </summary>
    public enum TransportType
    {
        /// <summary>
        /// Advanced Message Queuing Protocol transport.
        /// Try Amqp over TCP first and fallback to Amqp over WebSocket if that fails
        /// </summary>
        Amqp = 0,

        /// <summary>
        /// HyperText Transfer Protocol version 1 transport.
        /// </summary>
        Http1 = 1,

        /// <summary>
        /// Advanced Message Queuing Protocol transport over WebSocket only.
        /// </summary>
        AmqpWebSocketOnly = 2,

        /// <summary>
        /// Advanced Message Queuing Protocol transport over native TCP only
        /// </summary>
        AmqpTcpOnly = 3,

        /// <summary>
        /// Message Queuing Telemetry Transport.
        /// </summary>
        Mqtt = 4
    }

    /// <summary>
    /// Contains methods that a device can use to send messages to and receive from the service.
    /// </summary>
    public sealed class DeviceClient
#if !WINDOWS_UWP && !PCL
        : IDisposable
#endif
    {
        const string DeviceId = "DeviceId";
        const string DeviceIdParameterPattern = @"(^\s*?|.*;\s*?)" + DeviceId + @"\s*?=.*";
#if !PCL
        const RegexOptions RegexOptions = System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase;
#else
        const RegexOptions RegexOptions = System.Text.RegularExpressions.RegexOptions.IgnoreCase;
#endif
        static readonly Regex DeviceIdParameterRegex = new Regex(DeviceIdParameterPattern, RegexOptions);

        internal readonly IDelegatingHandler innerHandler;

#if !WINDOWS_UWP && !PCL
        DeviceClient(IotHubConnectionString iotHubConnectionString, ITransportSettings[] transportSettings)
        {
            DeviceStore.Clients.AddOrUpdate(iotHubConnectionString.ToString(), s => new ConcurrentQueue<DeviceClient>(new[] { this }), (s, t) =>
            {
                t.Enqueue(this);
                return t;
            });

            this.TransportHandlerFactory = this.CreateTransportHandler;
            this.innerHandler = new GatekeeperHandler
            {
                InnerHandler = new ErrorHandler(() => new RoutingHandler(this.TransportHandlerFactory, iotHubConnectionString, transportSettings))
            };
        }

        internal RoutingHandler.TransportHandlerFactory TransportHandlerFactory { get; set; }

        DeviceClientDelegatingHandler CreateTransportHandler(IotHubConnectionString iotHubConnectionString, ITransportSettings transportSetting)
        {
            switch (transportSetting.GetTransportType())
            {
                case TransportType.AmqpWebSocketOnly:
                case TransportType.AmqpTcpOnly:
                    return new AmqpTransportHandler(iotHubConnectionString, transportSetting as AmqpTransportSettings);
                case TransportType.Http1:
                    return new HttpTransportHandler(iotHubConnectionString, transportSetting as Http1TransportSettings);
                case TransportType.Mqtt:
                    return new MqttTransportHandler(iotHubConnectionString, transportSetting as MqttTransportSettings);
                default:
                    throw new InvalidOperationException("Unsupported Transport Setting {0}".FormatInvariant(transportSetting));
            }
        }

#else
        DeviceClient(IotHubConnectionString iotHubConnectionString)
        {
            this.innerHandler = new GatekeeperHandler
            {
                InnerHandler = new ErrorHandler(()=> new HttpTransportHandler(iotHubConnectionString))
            };
        }
#endif

        /// <summary>
        /// Create an Amqp DeviceClient from individual parameters
        /// </summary>
        /// <param name="hostname">The fully-qualified DNS hostname of IoT Hub</param>
        /// <param name="authenticationMethod">The authentication method that is used</param>
        /// <returns>DeviceClient</returns>
        public static DeviceClient Create(string hostname, IAuthenticationMethod authenticationMethod)
        {
#if WINDOWS_UWP || PCL
            return Create(hostname, authenticationMethod, TransportType.Http1);
#else
            return Create(hostname, authenticationMethod, TransportType.Amqp);
#endif
        }

        /// <summary>
        /// Create a DeviceClient from individual parameters
        /// </summary>
        /// <param name="hostname">The fully-qualified DNS hostname of IoT Hub</param>
        /// <param name="authenticationMethod">The authentication method that is used</param>
        /// <param name="transportType">The transportType used (Http1 or Amqp)</param>
        /// <returns>DeviceClient</returns>
        public static DeviceClient Create(string hostname, IAuthenticationMethod authenticationMethod, TransportType transportType)
        {
            if (hostname == null)
            {
                throw new ArgumentNullException(nameof(hostname));
            }

            if (authenticationMethod == null)
            {
                throw new ArgumentNullException(nameof(authenticationMethod));
            }

            IotHubConnectionStringBuilder connectionStringBuilder = IotHubConnectionStringBuilder.Create(hostname, authenticationMethod);
            return CreateFromConnectionString(connectionStringBuilder.ToString(), transportType);
        }

        /// <summary>
        /// Create a DeviceClient using Amqp transport from the specified connection string
        /// </summary>
        /// <param name="connectionString">Connection string for the IoT hub (including DeviceId)</param>
        /// <returns>DeviceClient</returns>
        public static DeviceClient CreateFromConnectionString(string connectionString)
        {
#if WINDOWS_UWP || PCL
            return CreateFromConnectionString(connectionString, TransportType.Http1);
#else
            return CreateFromConnectionString(connectionString, TransportType.Amqp);
#endif

        }

        /// <summary>
        /// Create a DeviceClient using Amqp transport from the specified connection string
        /// (WinRT) Create a DeviceClient using Http transport from the specified connection string
        /// </summary>
        /// <param name="connectionString">IoT Hub-Scope Connection string for the IoT hub (without DeviceId)</param>
        /// <param name="deviceId">Id of the Device</param>
        /// <returns>DeviceClient</returns>
        public static DeviceClient CreateFromConnectionString(string connectionString, string deviceId)
        {
#if WINDOWS_UWP || PCL
            return CreateFromConnectionString(connectionString, deviceId, TransportType.Http1);    
#else
            return CreateFromConnectionString(connectionString, deviceId, TransportType.Amqp);
#endif
        }

#if WINDOWS_UWP
        [Windows.Foundation.Metadata.DefaultOverloadAttribute()]
#endif

        /// <summary>
        /// Create DeviceClient from the specified connection string using the specified transport type
        /// (PCL) Only Http transport is allowed
        /// </summary>
        /// <param name="connectionString">Connection string for the IoT hub (including DeviceId)</param>
        /// <param name="transportType">Specifies whether Amqp or Http transport is used</param>
        /// <returns>DeviceClient</returns>
        public static DeviceClient CreateFromConnectionString(string connectionString, TransportType transportType)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            switch (transportType)
            {
                case TransportType.Amqp:
#if PCL
                    throw new NotImplementedException("Amqp protocol is not supported");
#else
                    return CreateFromConnectionString(connectionString, new ITransportSettings[]
                    {
                        new AmqpTransportSettings(TransportType.AmqpTcpOnly),
                        new AmqpTransportSettings(TransportType.AmqpWebSocketOnly)
                    });
#endif
                case TransportType.Mqtt:
#if WINDOWS_UWP || PCL
                    throw new NotImplementedException("Mqtt protocol is not supported");
#else
                    return CreateFromConnectionString(connectionString, new ITransportSettings[] { new MqttTransportSettings(transportType) });
#endif
                case TransportType.AmqpWebSocketOnly:
                case TransportType.AmqpTcpOnly:
#if PCL
                    throw new NotImplementedException("Amqp protocol is not supported");
#else
                    return CreateFromConnectionString(connectionString, new ITransportSettings[] { new AmqpTransportSettings(transportType) });
#endif
                case TransportType.Http1:
#if PCL
                    IotHubConnectionString iotHubConnectionString = IotHubConnectionString.Parse(connectionString);
                    return new DeviceClient(iotHubConnectionString);
#else
                    return CreateFromConnectionString(connectionString, new ITransportSettings[] { new Http1TransportSettings() });
#endif
                default:
#if !PCL
                    throw new InvalidOperationException("Unsupported Transport Type {0}".FormatInvariant(transportType));
#else
                    throw new InvalidOperationException($"Unsupported Transport Type {transportType}");
#endif
            }
        }

        /// <summary>
        /// Create DeviceClient from the specified connection string using the specified transport type
        /// </summary>
        /// <param name="connectionString">IoT Hub-Scope Connection string for the IoT hub (without DeviceId)</param>
        /// <param name="deviceId">Id of the device</param>
        /// <param name="transportType">The transportType used (Http1 or Amqp)</param>
        /// <returns>DeviceClient</returns>
        public static DeviceClient CreateFromConnectionString(string connectionString, string deviceId, TransportType transportType)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (deviceId == null)
            {
                throw new ArgumentNullException(nameof(deviceId));
            }

            if (DeviceIdParameterRegex.IsMatch(connectionString))
            {
                throw new ArgumentException("Connection string must not contain DeviceId keyvalue parameter", nameof(connectionString));
            }

            return CreateFromConnectionString(connectionString + ";" + DeviceId + "=" + deviceId, transportType);
        }

#if !PCL
        /// <summary>
        /// Create DeviceClient from the specified connection string using a prioritized list of transports
        /// </summary>
        /// <param name="connectionString">Connection string for the IoT hub (with DeviceId)</param>
        /// <param name="transportSettings">Prioritized list of transports</param>
        /// <returns>DeviceClient</returns>
        public static DeviceClient CreateFromConnectionString(string connectionString, [System.Runtime.InteropServices.WindowsRuntime.ReadOnlyArrayAttribute] ITransportSettings[] transportSettings)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (transportSettings == null)
            {
                throw new ArgumentNullException(nameof(transportSettings));
            }

            if (transportSettings.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(connectionString), "Must specify at least one TransportSettings instance");
            }

            IotHubConnectionString iotHubConnectionString = IotHubConnectionString.Parse(connectionString);

            foreach (ITransportSettings transportSetting in transportSettings)
            {
                switch (transportSetting.GetTransportType())
                {
                    case TransportType.AmqpWebSocketOnly:
                    case TransportType.AmqpTcpOnly:
                        if (!(transportSetting is AmqpTransportSettings))
                        {
                            throw new InvalidOperationException("Unknown implementation of ITransportSettings type");
                        }
                        break;
                    case TransportType.Http1:
                        if (!(transportSetting is Http1TransportSettings))
                        {
                            throw new InvalidOperationException("Unknown implementation of ITransportSettings type");
                        }
                        break;
#if !WINDOWS_UWP
                    case TransportType.Mqtt:
                        if (!(transportSetting is MqttTransportSettings))
                        {
                            throw new InvalidOperationException("Unknown implementation of ITransportSettings type");
                        }
                        break;
#endif
                    default:
                        throw new InvalidOperationException("Unsupported Transport Type {0}".FormatInvariant(transportSetting.GetTransportType()));
                }
            }

#if !WINDOWS_UWP
            // Defer concrete DeviceClient creation to OpenAsync
            return new DeviceClient(iotHubConnectionString, transportSettings);
#else
            return new DeviceClient(iotHubConnectionString);
#endif
        }

        /// <summary>
        /// Create DeviceClient from the specified connection string using the prioritized list of transports
        /// </summary>
        /// <param name="connectionString">Connection string for the IoT hub (without DeviceId)</param>
        /// <param name="deviceId">Id of the device</param>
        /// <param name="transportSettings">Prioritized list of transportTypes</param>
        /// <returns>DeviceClient</returns>
#if WINDOWS_UWP
        [Windows.Foundation.Metadata.DefaultOverloadAttribute]
#endif
        public static DeviceClient CreateFromConnectionString(string connectionString, string deviceId, [System.Runtime.InteropServices.WindowsRuntime.ReadOnlyArrayAttribute] ITransportSettings[] transportSettings)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (deviceId == null)
            {
                throw new ArgumentNullException(nameof(deviceId));
            }

            if (DeviceIdParameterRegex.IsMatch(connectionString))
            {
                throw new ArgumentException("Connection string must not contain DeviceId keyvalue parameter", nameof(connectionString));
            }

            return CreateFromConnectionString(connectionString + ";" + DeviceId + "=" + deviceId, transportSettings);
        }
#endif

            /// <summary>
            /// Explicitly open the DeviceClient instance.
            /// </summary>
        public AsyncTask OpenAsync()
        {
            return this.innerHandler.OpenAsync(true).AsTaskOrAsyncOp();
        }

        /// <summary>
        /// Close the DeviceClient instance
        /// </summary>
        /// <returns></returns>
        public AsyncTask CloseAsync()
        {
            return this.innerHandler.CloseAsync().AsTaskOrAsyncOp();
        }

        /// <summary>
        /// Receive a message from the device queue using the default timeout.
        /// </summary>
        /// <returns>The receive message or null if there was no message until the default timeout</returns>
        public AsyncTaskOfMessage ReceiveAsync()
        {
            return this.innerHandler.ReceiveAsync().AsTaskOrAsyncOp();
        }

        /// <summary>
        /// Receive a message from the device queue with the specified timeout
        /// </summary>
        /// <returns>The receive message or null if there was no message until the specified time has elapsed</returns>
        public AsyncTaskOfMessage ReceiveAsync(TimeSpan timeout)
        {
            return this.innerHandler.ReceiveAsync(timeout).AsTaskOrAsyncOp();
        }

#if WINDOWS_UWP
        [Windows.Foundation.Metadata.DefaultOverloadAttribute()]
#endif

        /// <summary>
        /// Deletes a received message from the device queue
        /// </summary>
        /// <returns>The lock identifier for the previously received message</returns>
        public AsyncTask CompleteAsync(string lockToken)
        {
            if (string.IsNullOrEmpty(lockToken))
            {
                throw Fx.Exception.ArgumentNull("lockToken");
            }

            return this.innerHandler.CompleteAsync(lockToken).AsTaskOrAsyncOp();
        }

        /// <summary>
        /// Deletes a received message from the device queue
        /// </summary>
        /// <returns>The previously received message</returns>
        public AsyncTask CompleteAsync(Message message)
        {
            if (message == null)
            {
                throw Fx.Exception.ArgumentNull("message");
            }

            return CompleteAsync(message.LockToken);
        }

#if WINDOWS_UWP
        [Windows.Foundation.Metadata.DefaultOverloadAttribute()]
#endif

        /// <summary>
        /// Puts a received message back onto the device queue
        /// </summary>
        /// <returns>The previously received message</returns>
        public AsyncTask AbandonAsync(string lockToken)
        {
            if (string.IsNullOrEmpty(lockToken))
            {
                throw Fx.Exception.ArgumentNull("lockToken");
            }

            return this.innerHandler.AbandonAsync(lockToken).AsTaskOrAsyncOp();
        }

        /// <summary>
        /// Puts a received message back onto the device queue
        /// </summary>
        /// <returns>The lock identifier for the previously received message</returns>
        public AsyncTask AbandonAsync(Message message)
        {
            if (message == null)
            {
                throw Fx.Exception.ArgumentNull("message");
            }

            return AbandonAsync(message.LockToken);
        }

#if WINDOWS_UWP
        [Windows.Foundation.Metadata.DefaultOverloadAttribute()]
#endif

        /// <summary>
        /// Deletes a received message from the device queue and indicates to the server that the message could not be processed.
        /// </summary>
        /// <returns>The previously received message</returns>
        public AsyncTask RejectAsync(string lockToken)
        {
            if (string.IsNullOrEmpty(lockToken))
            {
                throw Fx.Exception.ArgumentNull("lockToken");
            }

            return this.innerHandler.RejectAsync(lockToken).AsTaskOrAsyncOp();
        }

        /// <summary>
        /// Deletes a received message from the device queue and indicates to the server that the message could not be processed.
        /// </summary>
        /// <returns>The lock identifier for the previously received message</returns>
        public AsyncTask RejectAsync(Message message)
        {
            if (message == null)
            {
                throw Fx.Exception.ArgumentNull("message");
            }

            return this.RejectAsync(message.LockToken);
        }

        /// <summary>
        /// Sends an event to device hub
        /// </summary>
        /// <returns>The message containing the event</returns>
        public AsyncTask SendEventAsync(Message message)
        {
            if (message == null)
            {
                throw Fx.Exception.ArgumentNull("message");
            }

            return this.innerHandler.SendEventAsync(message).AsTaskOrAsyncOp();
        }

        /// <summary>
        /// Sends a batch of events to device hub
        /// </summary>
        /// <returns>The task containing the event</returns>
        public AsyncTask SendEventBatchAsync(IEnumerable<Message> messages)
        {
            if (messages == null)
            {
                throw Fx.Exception.ArgumentNull("messages");
            }

            return this.innerHandler.SendEventAsync(messages).AsTaskOrAsyncOp();
        }

        public void Dispose()
        {
            this.innerHandler?.Dispose();
        }
    }
}
