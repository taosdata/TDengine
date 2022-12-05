using Confluent.Kafka;
using Newtonsoft.Json;

namespace Producer
{
    internal class Produce
    {

        public async Task ProduceAsync(string bootstrapServer, string schemaRegisterUrl, string topicName)
        {
            Console.WriteLine("Press any key to begin tasks...");
            Console.ReadKey(true);
            Console.WriteLine("To terminate the example, press 'c' to cancel and exit...");
            Console.WriteLine();

            var produceConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
                //BootstrapServers = bootstrapServer;
            };

            MessageGenerator generator = new MessageGenerator();
            using (var p = new ProducerBuilder<string, string>(produceConfig).Build())
            {
                Console.WriteLine($"{p.Name} producing on {topicName}.");


                while (true)
                {
                    try
                    {
                        var dr = await p.ProduceAsync(topicName, new Message<string, string> { Key = JsonConvert.SerializeObject(generator.GenKey()), Value = JsonConvert.SerializeObject(generator.GenValue()) });
                        Console.WriteLine("{0} Thread {4}# Delivered key:{1} value:{2} to {3}", DateTimeOffset.UtcNow, dr.Key, dr.Value, dr.TopicPartitionOffset, Thread.CurrentThread.ManagedThreadId);
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                    if (generator.ts > 1667232550000)
                                       
                    {
                        break;
                    }

                }
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }

}


internal class ProduceResult
{
    internal string ID { get; set; }
    internal TimeSpan Cost { get; set; }

    internal long Count { get; set; }

}



