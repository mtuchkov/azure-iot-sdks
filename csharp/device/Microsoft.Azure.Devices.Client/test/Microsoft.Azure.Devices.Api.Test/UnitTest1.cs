using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.Devices.Client.Test
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    [TestClass]
    public class DeviceClientStressTest
    {
        static int counter;

        [TestMethod]
        public async Task CreateTonsOfClientsSuccessfully()
        {
            var clientTests = new List<Task>();
            for (int i = 0; i < 2; i++)
            {
                Interlocked.Increment(ref counter);
                clientTests.Add(RunOpenCloseTest(i));
            }

            await Task.WhenAll(clientTests);

            await Task.Delay(TimeSpan.FromSeconds(20));
        }

        static async Task RunOpenCloseTest(int i)
        {
            using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString($"HostName=mtuchkov-mqtt.azure-devices.net;DeviceId=dd{i};SharedAccessKey=sHxvBAjxU0ydNh9dZwjf3W5l6MBItKQvDCTAQVYuJh4=;GatewayHostName=ssl://mtuchkov-mqtt:8883", TransportType.Mqtt))
            {
                var message = new Message {CorrelationId = i.ToString()};
                await deviceClient.SendEventAsync(message);
                await deviceClient.CloseAsync();
            }
            if (Interlocked.Decrement(ref counter) == 0)
            {
                
            }
        }
    }
}
