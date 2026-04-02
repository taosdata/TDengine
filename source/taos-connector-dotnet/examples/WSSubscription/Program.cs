using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;

namespace WSSubscription
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var builder = new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec("CREATE DATABASE power");
                    client.Exec("USE power");
                    client.Exec(
                        "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
                    client.Exec("CREATE TOPIC topic_meters as SELECT * from power.meters");
                    var cfg = new Dictionary<string, string>()
                    {
                        { "td.connect.type", "WebSocket" },
                        { "group.id", "group1" },
                        { "auto.offset.reset", "latest" },
                        { "td.connect.ip", "localhost" },
                        { "td.connect.port", "6041" },
                        { "useSSL", "false" },
                        { "td.connect.user", "root" },
                        { "td.connect.pass", "taosdata" },
                        { "client.id", "tmq_example" },
                        { "enable.auto.commit", "true" },
                        { "msg.with.table.name", "false" },
                    };
                    var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                    consumer.Subscribe(new List<string>() { "topic_meters" });
                    Task.Run(InsertData);
                    while (true)
                    {
                        using (var cr = consumer.Consume(500))
                        {
                            if (cr == null) continue;
                            foreach (var message in cr.Message)
                            {
                                Console.WriteLine(
                                    $"message {{{((DateTime)message.Value["ts"]).ToString("yyyy-MM-dd HH:mm:ss.fff")}, " +
                                    $"{message.Value["current"]}, {message.Value["voltage"]}, {message.Value["phase"]}}}");
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    throw;
                }
            }
        }

        static void InsertData()
        {
            var builder = new ConnectionStringBuilder("protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
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
    }
}