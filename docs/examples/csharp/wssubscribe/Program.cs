using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;

namespace TMQExample
{
    internal class SubscribeDemo
    {
        private static string _host = "";
        private static string _groupId = "";
        private static string _clientId = "";
        private static string _topic = "";

        public static void Main(string[] args)
        {
            try
            {
                var builder =
                    new ConnectionStringBuilder(
                        "protocol=WebSocket;host=127.0.0.1;port=6041;username=root;password=taosdata");
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
            var builder =
                new ConnectionStringBuilder(
                    "protocol=WebSocket;host=127.0.0.1;port=6041;username=root;password=taosdata");
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
            _host = "127.0.0.1";
            _groupId = "group1";
            _clientId = "client1";
            var cfg = new Dictionary<string, string>()
            {
                { "td.connect.type", "WebSocket" },
                { "td.connect.port", "6041" },
                { "auto.offset.reset", "latest" },
                { "msg.with.table.name", "true" },
                { "enable.auto.commit", "true" },
                { "auto.commit.interval.ms", "1000" },
                { "group.id", _groupId },
                { "client.id", _clientId },
                { "td.connect.ip", _host },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
            };
            IConsumer<Dictionary<string, object>> consumer = null!;
            try
            {
                // create consumer
                consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                Console.WriteLine(
                    $"Create consumer successfully, " +
                    $"host: {_host}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}");
            }
            catch (TDengineError e)
            {
                // handle TDengine error
                Console.WriteLine(
                    $"Failed to create native consumer, " +
                    $"host: {_host}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrCode: {e.Code}, " +
                    $"ErrMessage: {e.Error}");
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine(
                    $"Failed to create native consumer, " +
                    $"host: {_host}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrMessage: {e.Message}");
                throw;
            }

            // ANCHOR_END: create_consumer
            return consumer;
        }

        static void Consume(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: subscribe
            _topic = "topic_meters";
            try
            {
                // subscribe
                consumer.Subscribe(new List<string>() { _topic });
                Console.WriteLine("Subscribe topics successfully");
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
                Console.WriteLine(
                    $"Failed to poll data, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrCode: {e.Code}, " +
                    $"ErrMessage: {e.Error}");
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine($"Failed to poll data, " +
                                  $"topic: {_topic}, " +
                                  $"groupId: {_groupId}, " +
                                  $"clientId: {_clientId}, " +
                                  $"ErrMessage: {e.Message}");
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
                Console.WriteLine($"Now assignment: {assignment}");
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
                Console.WriteLine(
                    $"Failed to seek offset, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"offset: 0, " +
                    $"ErrCode: {e.Code}, " +
                    $"ErrMessage: {e.Error}");
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine(
                    $"Failed to seek offset, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"offset: 0, " +
                    $"ErrMessage: {e.Message}");
                throw;
            }
            // ANCHOR_END: seek
        }

        static void CommitOffset(IConsumer<Dictionary<string, object>> consumer)
        {
            // ANCHOR: commit_offset
            for (int i = 0; i < 5; i++)
            {
                TopicPartitionOffset topicPartitionOffset = null;
                try
                {
                    // consume message with using block to ensure the result is disposed
                    using (var cr = consumer.Consume(100))
                    {
                        if (cr == null) continue;
                        // commit offset
                        topicPartitionOffset = cr.TopicPartitionOffset;
                        consumer.Commit(new List<TopicPartitionOffset>
                        {
                            topicPartitionOffset,
                        });
                        Console.WriteLine("Commit offset manually successfully.");
                    }
                }
                catch (TDengineError e)
                {
                    // handle TDengine error
                    Console.WriteLine(
                        $"Failed to commit offset, " +
                        $"topic: {_topic}, " +
                        $"groupId: {_groupId}, " +
                        $"clientId: {_clientId}, " +
                        $"offset: {topicPartitionOffset}, " +
                        $"ErrCode: {e.Code}, " +
                        $"ErrMessage: {e.Error}");
                    throw;
                }
                catch (Exception e)
                {
                    // handle other exceptions
                    Console.WriteLine(
                        $"Failed to commit offset, " +
                        $"topic: {_topic}, " +
                        $"groupId: {_groupId}, " +
                        $"clientId: {_clientId}, " +
                        $"offset: {topicPartitionOffset}, " +
                        $"ErrMessage: {e.Message}");
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
                Console.WriteLine(
                    $"Failed to unsubscribe consumer, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrCode: {e.Code}, " +
                    $"ErrMessage: {e.Error}");
                throw;
            }
            catch (Exception e)
            {
                // handle other exceptions
                Console.WriteLine(
                    $"Failed to execute commit example, " +
                    $"topic: {_topic}, " +
                    $"groupId: {_groupId}, " +
                    $"clientId: {_clientId}, " +
                    $"ErrMessage: {e.Message}");
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