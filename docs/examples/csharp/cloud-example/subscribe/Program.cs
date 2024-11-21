using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;

namespace Cloud.Examples
{
    public class SubscribeDemo
    {
        private static string _host = "";
        private static string _token = "";
        private static string _groupId = "";
        private static string _clientId = "";
        private static string _topic = "";

        static void Main(string[] args)
        {

            var cloudEndPoint = Environment.GetEnvironmentVariable("CLOUD_ENDPOINT");
            var cloudToken = Environment.GetEnvironmentVariable("CLOUD_TOKEN");
            _host = cloudEndPoint.ToString();
            _token = cloudToken.ToString();
            var connectionString = $"protocol=WebSocket;host={cloudEndPoint};port=443;useSSL=true;token={cloudToken};";
            // Connect to TDengine server using WebSocket
            var builder = new ConnectionStringBuilder(connectionString);
            try
            {
               // Open connection with using block, it will close the connection automatically
               using (var client = DbDriver.Open(builder))
               {
                  client.Exec("CREATE TOPIC IF NOT EXISTS topic_meters as SELECT * from test.meters");
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
               Console.WriteLine("Failed to insert to table meters using stmt, ErrCode: " + e.Code + ", ErrMessage: " + e.Error);
               throw;
            }
            catch (Exception e)
            {
               // handle other exceptions
               Console.WriteLine("Failed to insert to table meters using stmt, ErrMessage: " + e.Message);
               throw;
            }
        }

      static IConsumer<Dictionary<string, object>> CreateConsumer(){
          _groupId = "group1";
          _clientId = "client1";
          var cfg = new Dictionary<string, string>()
               {
                  { "td.connect.type", "WebSocket" },
                  { "group.id", _groupId },
                  { "auto.offset.reset", "latest" },
                  { "td.connect.ip", _host},
                  { "td.connect.port", "443" },
                  { "useSSL", "true" },
                  { "token", _token},
                  { "client.id", _clientId },
                  { "enable.auto.commit", "true" },
                  { "msg.with.table.name", "false" },
               };

         return new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
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
        static void InsertData()
        {
             var cloudEndPoint = Environment.GetEnvironmentVariable("CLOUD_ENDPOINT");
            var cloudToken = Environment.GetEnvironmentVariable("CLOUD_TOKEN");
            var connectionString = $"protocol=WebSocket;host={cloudEndPoint};port=443;useSSL=true;token={cloudToken};";
            // Connect to TDengine server using WebSocket
            var builder = new ConnectionStringBuilder(connectionString);
            using (var client = DbDriver.Open(builder))
            {
                while (true)
                {
                    client.Exec(
                        "INSERT into test.d991 using test.meters tags(2,'California.SanFrancisco') values(now,11.5,219,0.30)");
                    Task.Delay(1000).Wait();
                }
            }
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