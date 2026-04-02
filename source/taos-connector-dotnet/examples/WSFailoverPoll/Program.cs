using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;

namespace WSFailoverPoll
{
    internal class Program
    {
        private const string DatabaseName = "power_failover_poll";
        private const string StableName = "meters";
        private const string TableName = "d1001";
        private const string TopicName = "topic_meters_failover_poll";
        private const string Hosts = "127.0.0.1:6042,127.0.0.1:6043";

        public static void Main(string[] args)
        {
            var adminBuilder = new ConnectionStringBuilder(
                $"protocol=WebSocket;host={Hosts};useSSL=false;username=root;password=taosdata;autoReconnect=true;reconnectRetryCount=10;reconnectIntervalMs=200");

            using (var client = DbDriver.Open(adminBuilder))
            {
                PrepareTopic(client);
            }

            var cfg = new Dictionary<string, string>()
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", "group_failover_poll" },
                { "auto.offset.reset", "latest" },
                { "td.connect.ip", Hosts },
                { "useSSL", "false" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "client.id", "tmq_failover_poll_example" },
                { "enable.auto.commit", "true" },
                { "msg.with.table.name", "false" },
                { "ws.autoReconnect", "true" },
                { "ws.reconnect.retry.count", "10" },
                { "ws.reconnect.interval.ms", "200" },
            };

            var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
            consumer.Subscribe(new List<string>() { TopicName });
            Task.Run(InsertDataLoop);

            Console.WriteLine("Failover poll loop started. Stop one adapter to verify failover.");
            while (true)
            {
                try
                {
                    using (var cr = consumer.Consume(1000))
                    {
                        if (cr == null)
                        {
                            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} poll timeout");
                            continue;
                        }

                        foreach (var message in cr.Message)
                        {
                            Console.WriteLine(
                                $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} poll ok => ts={((DateTime)message.Value["ts"]).ToString("yyyy-MM-dd HH:mm:ss.fff")}, current={message.Value["current"]}, voltage={message.Value["voltage"]}, phase={message.Value["phase"]}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} poll failed => {ex.Message}");
                    Task.Delay(1000).Wait();
                }
            }
        }

        private static void PrepareTopic(ITDengineClient client)
        {
            client.Exec($"CREATE DATABASE IF NOT EXISTS {DatabaseName}");
            client.Exec($"CREATE STABLE IF NOT EXISTS {DatabaseName}.{StableName} (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
            client.Exec($"CREATE TOPIC IF NOT EXISTS {TopicName} AS SELECT * FROM {DatabaseName}.{StableName}");
        }

        private static void InsertDataLoop()
        {
            var builder = new ConnectionStringBuilder(
                $"protocol=WebSocket;host={Hosts};useSSL=false;username=root;password=taosdata;autoReconnect=true;reconnectRetryCount=10;reconnectIntervalMs=200");

            using (var client = DbDriver.Open(builder))
            {
                while (true)
                {
                    try
                    {
                        client.Exec(
                            $"INSERT INTO {DatabaseName}.{TableName} USING {DatabaseName}.{StableName} TAGS(1,'manual.failover') VALUES(now,11.5,219,0.30)");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} insert failed => {ex.Message}");
                    }

                    Task.Delay(1000).Wait();
                }
            }
        }
    }
}
