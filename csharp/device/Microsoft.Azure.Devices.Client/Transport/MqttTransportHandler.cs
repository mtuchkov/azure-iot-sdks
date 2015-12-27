// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Client.Transport
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading.Tasks;
    using DotNetty.Codecs.Mqtt;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Bootstrapping;
    using DotNetty.Transport.Channels;
    using DotNetty.Transport.Channels.Sockets;
    using Microsoft.Azure.Devices.ProtocolGateway.Mqtt;

    sealed class MqttTransportHandler : TansportHandlerBase
    {
        const int ProtocolGatewayPort = 8883;

        readonly Bootstrap bootstrap;
        readonly IPAddress serverAddress;
        Func<Task> cleanupFunc;
        
        internal MqttTransportHandler(IotHubConnectionString iotHubConnectionString)
        {
            string targetHost = iotHubConnectionString.HostName;
            
            IPHostEntry hostEntry = Dns.GetHostEntry(targetHost);

            this.serverAddress = hostEntry != null && hostEntry.AddressList.Length > 0 ? 
                hostEntry.AddressList[0] : IPAddress.Loopback;
                
            var group = new SingleInstanceEventLoopGroup();

            this.bootstrap = new Bootstrap()
                .Group(@group)
                .Channel<TcpSocketChannel>()
                .Option(ChannelOption.TcpNodelay, true)
                .Handler(new ActionChannelInitializer<ISocketChannel>(ch => ch.Pipeline.AddLast(
                    TlsHandler.Client(targetHost, null),
                    MqttEncoder.Instance,
                    new MqttDecoder(false, 256 * 1024),
                    new MqttIotHubAdapter(ta))));

            this.ScheduleCleanup(async () =>
            {
                await group.ShutdownGracefullyAsync();
            });

        }

        /// <summary>
        /// Create a DeviceClient from individual parameters
        /// </summary>
        /// <param name="hostname">The fully-qualified DNS hostname of IoT Hub</param>
        /// <param name="authMethod">The authentication method that is used</param>
        /// <returns>DeviceClient</returns>
        public static MqttTransportHandler Create(string hostname, IAuthenticationMethod authMethod)
        {
            if (hostname == null)
            {
                throw new ArgumentNullException("hostname");
            }

            if (authMethod == null)
            {
                throw new ArgumentNullException("authMethod");
            }

            var connectionStringBuilder = IotHubConnectionStringBuilder.Create(hostname, authMethod);
            return CreateFromConnectionString(connectionStringBuilder.ToString());
        }

        /// <summary>
        /// Create DeviceClient from the specified connection string
        /// </summary>
        /// <param name="connectionString">Connection string for the IoT hub</param>
        /// <returns>DeviceClient</returns>
        public static MqttTransportHandler CreateFromConnectionString(string connectionString)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException("connectionString");
            }

            var iotHubConnectionString = IotHubConnectionString.Parse(connectionString);
            
            return new MqttTransportHandler(iotHubConnectionString);
        }

        protected override TimeSpan DefaultReceiveTimeout { get; set; }

        protected override async Task OnOpenAsync(bool explicitOpen)
        {
            IChannel clientChannel = await this.bootstrap.ConnectAsync(this.serverAddress, ProtocolGatewayPort);
            
            this.ScheduleCleanup(async () =>
            {
                await clientChannel.CloseAsync();
            });
        }

        protected override Task OnCloseAsync()
        {
            
        }

        protected override Task OnSendEventAsync(Message message)
        {
            
        }

        protected override Task OnSendEventAsync(IEnumerable<Message> messages)
        {
            
        }

        internal Task SendEventAsync(IEnumerable<string> messages)
        {
            
        }

        internal Task SendEventAsync(IEnumerable<Tuple<string, IDictionary<string, string>>> messages)
        {
            
        }

        protected async override Task<Message> OnReceiveAsync(TimeSpan timeout)
        {
            
        }

        protected override Task OnCompleteAsync(string lockToken)
        {
            
        }

        protected override Task OnAbandonAsync(string lockToken)
        {
            
        }

        protected override Task OnRejectAsync(string lockToken)
        {
            
        }


        void ScheduleCleanup(Func<Task> cleanupFunc)
        {
            Func<Task> currentCleanupFunc = this.cleanupFunc;
            this.cleanupFunc = async () =>
            {
                if (currentCleanupFunc != null)
                {
                    await currentCleanupFunc();
                }

                await cleanupFunc();
            };
        }
    }

    internal class SingleInstanceEventLoopGroup : IEventLoopGroup
    {
        private readonly SingleThreadEventLoop eventLoop;

        public SingleInstanceEventLoopGroup()
        {
            this.eventLoop = new SingleThreadEventLoop();
        }

        public IEventLoop GetNext()
        {
            return this.eventLoop;
        }

        public Task ShutdownGracefullyAsync()
        {
            return this.eventLoop.ShutdownGracefullyAsync();
        }

        public Task TerminationCompletion
        {
            get
            {
                return this.eventLoop.TerminationCompletion;
            }
        }
    }
}