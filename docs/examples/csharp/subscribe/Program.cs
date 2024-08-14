using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;

namespace TMQExample
{
    internal class SubscribeDemo
    {
        public static void Main(string[] args)
        {
            try
            {
                var builder = new ConnectionStringBuilder("host=127.0.0.1;port=6030;username=root;password=taosdata");
                using (var client = DbDriver.Open(builder))
                {
                    client.Exec("CREATE DATABASE IF NOT EXISTS power");
                    client.Exec("USE power");
                    client.Exec(
                        "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                    client.Exec("CREATE TOPIC IF NOT EXISTS topic_meters as SELECT * from power.meters");
                    var consumer = CreateConsumer();
                    // insert data
                    Task.Run(InsertData);
                    // consume message
                    Consume(consumer);
                    // seek
                    Seek(consumer);
                    // commit
                    CommitOffset(consumer);
                    // close
                    Close(consumer);
                    Console.WriteLine("Done");
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine(e.Message);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine(e.Message);
                throw;
            }
        }

        static void InsertData()
        {
            var builder = new ConnectionStringBuilder("host=127.0.0.1;port=6030;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                while (true)
                {
                    client.Exec(
                        "INSERT into power.d1001 using power.meters tags(2,'California.SanFrancisco') values(now,11.5,219,0.30)");
                    Task.Delay(1000).Wait();
                }
            }
        }

        static IConsumer<Dictionary<string, object>> CreateConsumer()
        {
            // ANCHOR: create_consumer
            // consumer config
            var host = "127.0.0.1";
            var groupId = "group1";
            var clientId = "client1";
            var cfg = new Dictionary<string, string>()
            {
                { "td.connect.port", "6030" },
                { "auto.offset.reset", "latest" },
                { "msg.with.table.name", "true" },
                { "enable.auto.commit", "true" },
                { "auto.commit.interval.ms", "1000" },
                { "group.id", groupId },
                { "client.id", clientId },
                { "td.connect.ip", host },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
            };
            IConsumer<Dictionary<string, object>> consumer = null!;
            try
            {
                // create consumer
                consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                Console.WriteLine("Create consumer successfully, host: " + host + ", groupId: " + groupId +
                                  ", clientId: " + clientId);
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to create native consumer, host: " + host + ", ErrCode:" + e.Code +
                                  ", ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to create native consumer, host: " + host + ", ErrMessage: " + e.Message);
                throw;
            }

            // ANCHOR_END: create_consumer
            return consumer;
        }

        static void Consume(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: subscribe
            try
            {
                // subscribe
                consumer.Subscribe(new List<string>() { "topic_meters" });
                Console.WriteLine("subscribe topics successfully");
                for (int i = 0; i < 50; i++)
                {
                    // consume message with using block to ensure the result is disposed
                    using (var cr = consumer.Consume(100))
                    {
                        if (cr == null) continue;
                        foreach (var message in cr.Message)
                        {
                            // handle message
                            Console.WriteLine(
                                $"data: {{{((DateTime)message.Value["ts"]).ToString("yyyy-MM-dd HH:mm:ss.fff")}, " +
                                $"{message.Value["current"]}, {message.Value["voltage"]}, {message.Value["phase"]}}}");
                        }
                    }
                }
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to poll data, ErrCode: " + e.Code + ", ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to poll data, ErrMessage: " + e.Message);
                throw;
            }
            // ANCHOR_END: subscribe
        }

        static void Seek(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: seek
            try
            {
                // get assignment
                var assignment = consumer.Assignment;
                Console.WriteLine($"now assignment: {assignment}");
                // seek to the beginning
                foreach (var topicPartition in assignment)
                {
                    consumer.Seek(new TopicPartitionOffset(topicPartition.Topic, topicPartition.Partition, 0));
                }
                Console.WriteLine("Assignment seek to beginning successfully");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to execute seek example, ErrCode: " + e.Code + ", ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to execute seek example, ErrMessage: " + e.Message);
                throw;
            }
            // ANCHOR_END: seek
        }

        static void CommitOffset(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: commit_offset
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    // consume message with using block to ensure the result is disposed
                    using (var cr = consumer.Consume(100))
                    {
                        if (cr == null) continue;
                        // commit offset
                        consumer.Commit(new List<TopicPartitionOffset>
                        {
                            cr.TopicPartitionOffset,
                        });
                        Console.WriteLine("Commit offset manually successfully.");
                    }
                }
                catch (TDengineError e)
                {
                    // handle TDengine error
                    Console.WriteLine("Failed to execute commit example, ErrCode:" + e.Code + ", ErrMessage: " + e.Error);
                    throw;
                }
                catch (Exception e)
                {
                    // handle other exceptions
                    Console.WriteLine("Failed to execute commit example, ErrMessage: " + e.Message);
                    throw;
                }
            }
            // ANCHOR_END: commit_offset
        }

        static void Close(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: close
            try
            {
                // unsubscribe
                consumer.Unsubscribe();
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine("Failed to unsubscribe consumer, ErrCode: " + e.Code + ", ErrMessage: " + e.Error);
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine("Failed to unsubscribe consumer, ErrMessage: " + e.Message);
                throw;
            }
            finally
            {
                // close consumer
                consumer.Close();
                Console.WriteLine("Consumer closed successfully.");
            }
            // ANCHOR_END: close
        }
    }
}
