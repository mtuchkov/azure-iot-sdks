using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.Devices.Client.Test
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestClass]
    public class DeviceClientStressTest
    {
        [TestMethod]
        public async Task CreateTonsOfClientsSuccessfully()
        {
            var clientTests = new List<Task>();
            for (int i = 0; i < 100; i++)
            {
                clientTests.Add(RunOpenCloseTest());
            }

            await Task.WhenAll(clientTests);

            await Task.Delay(TimeSpan.FromSeconds(20));
        }

        static async Task RunOpenCloseTest()
        {
            using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString("HostName=mtuchkov-mqtt.azure-devices.net;DeviceId=demodevice;SharedAccessKey=sHxvBAjxU0ydNh9dZwjf3W5l6MBItKQvDCTAQVYuJh4=;GatewayHostName=ssl://mtuchkov-mqtt:8883", TransportType.Mqtt))
            {
                await deviceClient.OpenAsync();
            }
        }
    }
}
