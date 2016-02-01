// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------
namespace Microsoft.Azure.Devices.Client.Test
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Common;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using TransportType = Microsoft.Azure.Devices.Client.TransportType;

    [TestClass]
    public class MqttTests
    {
        const string CommandQoS0Content = "{\"test\": \"notify (at most once)\"}";
        const string CommandQoS1Content = "{\"test\": \"notify (at least once)\"}";
        const int MqttPort = 8883;

        private static readonly ConcurrentQueue<TaskCompletionSource<int>> SendCommandCompletionSources = new ConcurrentQueue<TaskCompletionSource<int>>();

        private const string IoTHubName = "acme-mtuchkov";
        private const string HostSuffix = "private.azure-devices-int.net";
        private const string DeviceId = "device1";
        private const string PrimaryKey = "cXtN0iOTjeY7QPuEGHtPA8ZVQ+Jj5C6iHTEo2yCV0eM=";
        private const string KeyName = "iothubowner";

        private const string ScaleUnitHost = IoTHubName + "." + HostSuffix; //acme-mtuchkov.private.azure-devices-int.net";
        private const string UserName = ScaleUnitHost + "/" + DeviceId; //"acme-mtuchkov.private.azure-devices-int.net/64a14b41-77ab-4442-b093-9c857d2d2a29";


        [TestMethod] 
        public async Task E2ETest()
        {
            DeviceClient client = DeviceClient.CreateFromConnectionString("HostName=acme-mtuchkov.private.azure-devices-int.net;DeviceId=device1;SharedAccessKey=l3s/NXcR1oVBNMaJicX8CPVOy905WolzUSKZcYBIcpA=;", TransportType.Mqtt);
            await client.OpenAsync();

            await Task.Delay(3000);

            await client.SendEventAsync(new Message(Encoding.UTF8.GetBytes("Test1"))
            {
                MessageId = "1",
                CorrelationId = "c1",
                To = "r1"
            });
            await client.SendEventAsync(new Message(Encoding.UTF8.GetBytes("Test2")));

            await ReceiveNextMessageAsync(client);
            
            await client.CloseAsync();
        }

        static async Task ReceiveNextMessageAsync(DeviceClient client)
        {
            Message message = await client.ReceiveAsync(TimeSpan.FromMinutes(1));
            if (message.LockToken != null)
            {
                await client.CompleteAsync(message.LockToken);
            }
        }

        private static async Task SendCommandsAsync(ServiceClient serviceClient)
        {
            await SendCommandAsync(serviceClient, 0, CommandQoS0Content);
            await SendCommandAsync(serviceClient, 1, CommandQoS1Content);
        }

        static async Task SendCommandAsync(ServiceClient serviceClient, byte qos, string messageContent)
        {
            await SendCommandAndTrackAsync(async () =>
            {
                var message = new Devices.Message(Encoding.UTF8.GetBytes(messageContent));
                
                await serviceClient.SendAsync(DeviceId, message);

                Console.WriteLine("Message sent:");
                Console.WriteLine("QoS: " + qos);
                Console.WriteLine("Content: " + messageContent);
                Console.WriteLine();
            });
        }
        static async Task SendCommandAndTrackAsync(Func<Task> sendCommandTask)
        {
            var completion = new TaskCompletionSource<int>();
            SendCommandCompletionSources.Enqueue(completion);
            await sendCommandTask();
            completion.SetResult(0);
        }

        private static async Task WaitForReceiveCompletedAsync()
        {
            TaskCompletionSource<int> completion;
            while (SendCommandCompletionSources.TryPeek(out completion))
            {
                await completion.Task;
            }
        }

        static string GetServiceConnectionString()
        {
            var defaultDevice = new Device("mydeviceid") { Authentication = new AuthenticationMechanism {SymmetricKey = new SymmetricKey {PrimaryKey = PrimaryKey, SecondaryKey = PrimaryKey} } };
            string hostname = "{0}.{1}".FormatInvariant(IoTHubName, HostSuffix);
            var authMethod = new ServiceAuthenticationWithSharedAccessPolicyKey(KeyName, PrimaryKey);
            var iotHubScopeAllAccessConnectionStringBuilder = Devices.IotHubConnectionStringBuilder.Create(hostname, authMethod);
            return iotHubScopeAllAccessConnectionStringBuilder.ToString();
        }

        static async Task<string> GetCurrentEventHubOffsetAsync(EventHubClient eventHubClient)
        {
            var eventHubReceiver = CreateEventHubReceiver(eventHubClient);
            var eventDatas = (await eventHubReceiver.ReceiveAsync(1000, TimeSpan.FromSeconds(30))).ToArray();
            return eventDatas[eventDatas.Length - 1].Offset;
        }

        static async Task ReceiveTelemtryFromEventHubAsync(EventHubClient eventHubClient, string offset)
        {
            var eventHubReceiver = CreateEventHubReceiver(eventHubClient, offset);
            var eventDatas = (await eventHubReceiver.ReceiveAsync(1000, TimeSpan.FromSeconds(30))).ToArray();
            foreach (var eventData in eventDatas)
            {
                Debug.WriteLine("Message received from EventHub:");
                Debug.WriteLine(Encoding.ASCII.GetString(eventData.GetBytes()));
                Debug.WriteLine("");
            }
        }

        static EventHubClient CreateEventHubClient()
        {
            var connectionStringbuilder = new ServiceBusConnectionStringBuilder();
            connectionStringbuilder.Endpoints.Add(
                new Uri("sb://mtuchkovsbnamespace.servicebus.windows.net/"));
            connectionStringbuilder.SharedAccessKeyName = "iothubowner";
            connectionStringbuilder.SharedAccessKey = "cXtN0iOTjeY7QPuEGHtPA8ZVQ+Jj5C6iHTEo2yCV0eM=";
            var eventHubName = "mtuchkovdhrp-iothub-ehub-acme-mtuch-1147-7f9ee64b80";
            var eventHubConnectionString = connectionStringbuilder.ToString();

            var eventHubClient = EventHubClient.CreateFromConnectionString(eventHubConnectionString, eventHubName);
            return eventHubClient;
        }

        private static EventHubReceiver CreateEventHubReceiver(EventHubClient eventHubClient, string offset = null)
        {
            var ehGroup = eventHubClient.GetDefaultConsumerGroup();
            var partitionId = PartitionKeyResolver.ResolvePartitionId(DeviceId, 4);
            var eventHubReceiver = offset != null ?
                ehGroup.CreateReceiver(eventHubClient.GetRuntimeInformation().PartitionIds[int.Parse(partitionId)], offset) : ehGroup.CreateReceiver(eventHubClient.GetRuntimeInformation().PartitionIds[int.Parse(partitionId)]);
            return eventHubReceiver;
        }
    }

    public static class PartitionKeyResolver
    {
        const short DefaultLogicalPartitionCount = 32767;

        public static string ResolvePartitionId(string partitionKey, int partitionCount)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException("partitionKey");
            }

            if (partitionCount < 1 || partitionCount > DefaultLogicalPartitionCount)
            {
                throw new ArgumentOutOfRangeException("partitionCount", partitionCount, string.Format(CultureInfo.InvariantCulture, "Should be between {0} and {1}", 1, DefaultLogicalPartitionCount));
            }

            short logicalPartition = Math.Abs((short)(PerfectHash.HashToShort(partitionKey) % DefaultLogicalPartitionCount));

            int shortRangeWidth = (int)Math.Floor((decimal)DefaultLogicalPartitionCount / partitionCount);
            int remainingLogicalPartitions = DefaultLogicalPartitionCount - (partitionCount * shortRangeWidth);
            int largeRangeWidth = shortRangeWidth + 1;
            int largeRangesLogicalPartitions = largeRangeWidth * remainingLogicalPartitions;
            int partitionIndex = logicalPartition < largeRangesLogicalPartitions
                ? logicalPartition / largeRangeWidth
                : remainingLogicalPartitions + ((logicalPartition - largeRangesLogicalPartitions) / shortRangeWidth);

            return partitionIndex.ToString(NumberFormatInfo.InvariantInfo);

        }
    }

    public static class PerfectHash
    {
        public static long HashToLong(string data)
        {
            uint hash1;
            uint hash2;
            PerfectHash.ComputeHash(Encoding.ASCII.GetBytes(data.ToUpper(CultureInfo.InvariantCulture)), 0U, 0U, out hash1, out hash2);
            return (long)hash1 << 32 | (long)hash2;
        }

        public static short HashToShort(string data)
        {
            uint hash1;
            uint hash2;
            PerfectHash.ComputeHash(Encoding.ASCII.GetBytes(data.ToUpper(CultureInfo.InvariantCulture)), 0U, 0U, out hash1, out hash2);
            return (short)(hash1 ^ hash2);
        }

        private static void ComputeHash(byte[] data, uint seed1, uint seed2, out uint hash1, out uint hash2)
        {
            int num1;
            uint num2 = (uint)(num1 = (int)(uint)(3735928559UL + (ulong)data.Length + (ulong)seed1));
            uint num3 = (uint)num1;
            uint num4 = (uint)num1;
            uint num5 = num2 + seed2;
            int startIndex = 0;
            int length = data.Length;
            while (length > 12)
            {
                uint num6 = num4 + BitConverter.ToUInt32(data, startIndex);
                uint num7 = num3 + BitConverter.ToUInt32(data, startIndex + 4);
                uint num8 = num5 + BitConverter.ToUInt32(data, startIndex + 8);
                uint num9 = num6 - num8 ^ (num8 << 4 | num8 >> 28);
                uint num10 = num8 + num7;
                uint num11 = num7 - num9 ^ (num9 << 6 | num9 >> 26);
                uint num12 = num9 + num10;
                uint num13 = num10 - num11 ^ (num11 << 8 | num11 >> 24);
                uint num14 = num11 + num12;
                uint num15 = num12 - num13 ^ (num13 << 16 | num13 >> 16);
                uint num16 = num13 + num14;
                uint num17 = num14 - num15 ^ (num15 << 19 | num15 >> 13);
                num4 = num15 + num16;
                num5 = num16 - num17 ^ (num17 << 4 | num17 >> 28);
                num3 = num17 + num4;
                startIndex += 12;
                length -= 12;
            }
            switch (length)
            {
                case 0:
                    hash1 = num5;
                    hash2 = num3;
                    return;
                case 1:
                    num4 += (uint)data[startIndex];
                    break;
                case 2:
                    num4 += (uint)data[startIndex + 1] << 8;
                    goto case 1;
                case 3:
                    num4 += (uint)data[startIndex + 2] << 16;
                    goto case 2;
                case 4:
                    num4 += BitConverter.ToUInt32(data, startIndex);
                    break;
                case 5:
                    num3 += (uint)data[startIndex + 4];
                    goto case 4;
                case 6:
                    num3 += (uint)data[startIndex + 5] << 8;
                    goto case 5;
                case 7:
                    num3 += (uint)data[startIndex + 6] << 16;
                    goto case 6;
                case 8:
                    num3 += BitConverter.ToUInt32(data, startIndex + 4);
                    num4 += BitConverter.ToUInt32(data, startIndex);
                    break;
                case 9:
                    num5 += (uint)data[startIndex + 8];
                    goto case 8;
                case 10:
                    num5 += (uint)data[startIndex + 9] << 8;
                    goto case 9;
                case 11:
                    num5 += (uint)data[startIndex + 10] << 16;
                    goto case 10;
                case 12:
                    num4 += BitConverter.ToUInt32(data, startIndex);
                    num3 += BitConverter.ToUInt32(data, startIndex + 4);
                    num5 += BitConverter.ToUInt32(data, startIndex + 8);
                    break;
            }
            uint num18 = (num5 ^ num3) - (num3 << 14 | num3 >> 18);
            uint num19 = (num4 ^ num18) - (num18 << 11 | num18 >> 21);
            uint num20 = (num3 ^ num19) - (num19 << 25 | num19 >> 7);
            uint num21 = (num18 ^ num20) - (num20 << 16 | num20 >> 16);
            uint num22 = (num19 ^ num21) - (num21 << 4 | num21 >> 28);
            uint num23 = (num20 ^ num22) - (num22 << 14 | num22 >> 18);
            uint num24 = (num21 ^ num23) - (num23 << 24 | num23 >> 8);
            hash1 = num24;
            hash2 = num23;
        }
    }
}