using System;
using System.Threading.Tasks;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

namespace MQTT_Publisher
{
    class Publisher
    {
        static async Task Main(string[] args)
        {
            var cnt = 0;
            var mqttFactory = new MqttFactory();
            IMqttClient client = mqttFactory.CreateMqttClient();


            while (cnt < 50)
            {
                while (!client.IsConnected && cnt < 50)
                {
                    try
                    {
                        var options = new MqttClientOptionsBuilder()
                               .WithClientId(Guid.NewGuid().ToString())
                               .WithTcpServer("192.168.110.183", 1883)
                               .WithCredentials("admin", "admin")
                               .WithCleanSession()
                               .Build();
                        await client.ConnectAsync(options);

                    }
                    catch (System.Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        Console.WriteLine("MQTT Broker Connection Failed");
                        await Task.Delay(TimeSpan.FromSeconds(8));
                    }
                }
                while (client.IsConnected && cnt < 50)
                {
                    Console.WriteLine("Broker Connected");
                    await PublishMessageAsync(client);
                    Console.WriteLine("Message Published");
                    await Task.Delay(500);
                    cnt++;
                }


            }
            await client.DisconnectAsync();


        }
        private static async Task PublishMessageAsync(IMqttClient client)
        {
            string MessagePayload = JsonSerializer.Serialize(new MessagePayload()
            {
                Value = RandomNumber(50.3, 255.4).ToString(),
                CreatedAt = DateTime.Now,

            });
            var message = new MqttApplicationMessageBuilder()
                               .WithTopic("tag/temperature")
                               .WithPayload(MessagePayload)
                               .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                               .Build();
            if (client.IsConnected)
            {
                await client.PublishAsync(message);
                Console.WriteLine(MessagePayload);
            }
        }
        private static double RandomNumber(double minimum, double maximum)
        {
            Random random = new Random();
            return random.NextDouble() * (maximum - minimum) + minimum;
        }


    }

    class MessagePayload
    {
        public string Value { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
