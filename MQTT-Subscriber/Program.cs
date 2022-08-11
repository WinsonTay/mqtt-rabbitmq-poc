using System;
using System.Threading.Tasks;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using System.Collections.Generic;
using MQTTnet.Protocol;
using MQTTnet.Packets;
using System.Text;


namespace MQTT_Subscriber
{
    public class Subscriber
    {
    
        static async Task Main(string[] args)
        {

            var mqttFactory = new MqttFactory();
            IMqttClient client = mqttFactory.CreateMqttClient();
            var options = new MqttClientOptionsBuilder()
                               .WithClientId(Guid.NewGuid().ToString())
                               .WithTcpServer("192.168.0.110", 1883)
                               .WithCredentials("admin", "admin")
                               .WithCleanSession()
                               .Build();
            client.DisconnectedAsync += Client_DisconnectedAsync;
            client.ApplicationMessageReceivedAsync += Client_ApplicationMessageReceivedAsync;

            while (true)
            {
                while (!client.IsConnected)
                {
                    try
                    {
                        await client.ConnectAsync(options);
                        var topicFilter = new MqttTopicFilterBuilder()
                           .WithTopic("tag/temperature")
                           .Build();
                        await client.SubscribeAsync(topicFilter);
                        Console.WriteLine("Broker Connected, Waiting for Publisher Message");
                    }
                    catch (Exception)
                    {

                        Console.WriteLine("Connect Broker Failed");
                        await Task.Delay(8000);
                    }
                }
                await Task.Delay(5000);
            }
            
            await client.DisconnectAsync();

        }

        private static Task Client_DisconnectedAsync(MqttClientDisconnectedEventArgs arg)
        {
            Console.WriteLine("Broker Connection Down .. ");
            return Task.CompletedTask;
        }

        private static Task Client_ApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs arg)
        {
            Console.WriteLine("Msg Received");
            try
            {
                    var msg = JsonSerializer.Deserialize<LiveTagMsgPayloadDto>(Encoding.UTF8.GetString(arg.ApplicationMessage.Payload));
                    Console.WriteLine($"{msg.EdgeId}");
                    msg.TagPayload.ForEach(t => Console.WriteLine($"Id:{t.Id}, Value:{t.Value}")); 
            }
            catch (System.Exception ex)
            {
                Console.WriteLine("Failed to Serialize");
                Console.WriteLine(ex.Message);
            }
       
            return Task.CompletedTask;
        }

    }
    class MessagePayload
    {
        public string Value { get; set; }
        public DateTime CreatedAt { get; set; }
    }
    public class LiveTagMsgPayloadDto
    {
        public string EdgeId { get; set; }
        public List<TagPayload> TagPayload {get; set;}
    }
        public class TagPayload
    {
        public string Id { get; set; }
        public string Value { get; set; }
        public string Unit { get; set; }
    }
}
