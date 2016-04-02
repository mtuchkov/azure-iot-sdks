using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.Devices.Client.Test
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DotNetty.Codecs.Mqtt.Packets;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;

    [TestClass]
    public class DeviceClientStressTest
    {
        long sendSuccessCounter;
        long sendFailureCounter;
        long receiveSuccessCounter;
        long receiveFailureCounter;

        readonly List<KeyValuePair<string,DeviceClient>> deviceClients = new List<KeyValuePair<string, DeviceClient>>(1000);
        readonly byte[] payload = Encoding.UTF8.GetBytes(new string('f', 1024));
        readonly ServiceClient serviceClient = ServiceClient.CreateFromConnectionString("HostName=mtuchkov-mqtt.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=Ura98bnRd10+oV+Rg6KUtobRkfLSQmPYAvPQxv4uyGc=");
        readonly ThreadLocal<Random> random = new ThreadLocal<Random>(() => new Random());
        [TestInitialize]
        public void TestInit()
        {
            for (int i = 0; i < 1000; i++)
            {
                this.deviceClients.Add(new KeyValuePair<string, DeviceClient>("mydevice_"+i, DeviceClient.CreateFromConnectionString(
                    $"HostName=mtuchkov-mqtt.azure-devices.net;DeviceId=mydevice_{i};SharedAccessKey=sHxvBAjxU0ydNh9dZwjf3W5l6MBItKQvDCTAQVYuJh4=;GatewayHostName=ssl://mtuchkov-mqtt:8883", TransportType.Mqtt)));
            }
            for (int i = 0; i < this.payload.Length; i++)
            {
                this.payload[i] = 0x24;
            }
        }

        [TestMethod]
        public async Task CreateTonsOfClientsSuccessfully()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var cts = new CancellationTokenSource();
            this.deviceClients.ForEach(pair => this.StartSendingTelemetry(pair, cts.Token));
            this.deviceClients.ForEach(pair => this.StartReceivingCommands(pair, cts.Token));

            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
                var sendSuccessRate = (double)Interlocked.Read(ref this.sendSuccessCounter) * 1000 / stopwatch.ElapsedMilliseconds;
                var sendFailureRate = (double)Interlocked.Read(ref this.sendFailureCounter) * 1000 / stopwatch.ElapsedMilliseconds;
                var receiveSuccessRate = (double)Interlocked.Read(ref this.receiveSuccessCounter) * 1000 / stopwatch.ElapsedMilliseconds;
                var receiveFailureRate = (double)Interlocked.Read(ref this.receiveFailureCounter) * 1000 / stopwatch.ElapsedMilliseconds;
                //for debug purposes only
                if (cts.Token.IsCancellationRequested)
                {
                    
                    cts.Cancel();
                }
            }
        }

        void StartSendingTelemetry(KeyValuePair<string, DeviceClient> device, CancellationToken token)
        {
            for (int i = 0; i < 17; i++)
            {
                Task.Run(()=> this.RunSendAsync(device, token));
            }
        }

        async Task RunSendAsync(KeyValuePair<string, DeviceClient> device, CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    await device.Value.SendEventAsync(new Message(this.payload));
                    Interlocked.Increment(ref this.sendSuccessCounter);
                    await Task.Delay(1, token);
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref this.sendFailureCounter);
            }
        }

        void StartReceivingCommands(KeyValuePair<string, DeviceClient> device, CancellationToken token)
        {
            for (int i = 0; i < 17; i++)
            {
                Task.Run(() => this.RunReceivingAsync(device, token));
            }
        }

        async Task RunReceivingAsync(KeyValuePair<string, DeviceClient> device, CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    int delay = this.random.Value.Next(0, 4000);

                    await Task.Delay(delay, token);

                    try
                    {
                        await serviceClient.SendAsync(device.Key, new Devices.Message(this.payload));
                    }
                    catch (Exception ex)
                    {
                        if (!ex.Message.StartsWith("Device Queue depth cannot exceed 50 messages"))
                        {
                            throw;
                        }
                    }
                    Message message = await device.Value.ReceiveAsync();

                    while (true)
                    {
                        try
                        {
                            await device.Value.CompleteAsync(message.LockToken);
                            break;
                        }
                        catch (Exception ex)
                        {
                            
                        }
                    }
                    Interlocked.Increment(ref this.receiveSuccessCounter);
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref this.receiveFailureCounter);
            }
        }
    }
}
