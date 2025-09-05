using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;
using Xunit;

namespace Driver.Test.Client.TMQ
{
    public partial class Consumer
    {

        [Fact]
        public void SubscribeReconnect()
        {
            var port = "36041";
            var process = Tools.TaosAdapterTools.NewTaosAdapter(port);
            Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
            try
            {
                var connStr =
                    $"protocol=WebSocket;host=127.0.0.1;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true";
                var builder = new ConnectionStringBuilder(connStr);
                using (var client = DbDriver.Open(builder))
                {
                    client.Exec("create database if not exists test_subscribe_reconnect");
                    client.Exec(
                        "create topic if not exists topic_subscribe_reconnect as database test_subscribe_reconnect");
                }

                var cfg = new Dictionary<string, string>()
                {
                    { "td.connect.type", "WebSocket" },
                    { "group.id", "test" },
                    { "auto.offset.reset", "earliest" },
                    { "td.connect.ip", "127.0.0.1" },
                    { "td.connect.user", "root" },
                    { "td.connect.pass", "taosdata" },
                    { "td.connect.port", $"{port}" },
                    { "client.id", "test_tmq_c" },
                    { "enable.auto.commit", "false" },
                    { "msg.with.table.name", "true" },
                    { "useSSL", "false" },
                    { "ws.autoReconnect", "true" },
                    { "ws.reconnect.retry.count", "3" },
                    { "ws.reconnect.interval.ms", "2000" }
                };
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                Tools.TaosAdapterTools.StopTaosAdapter(process);
                Task.Run(() =>
                {
                    Thread.Sleep(3000);
                    Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
                });
                consumer.Subscribe("topic_subscribe_reconnect");
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(process);
                process.Dispose();
            }
        }

        [Fact]
        public void ConsumeReconnect()
        {
            var port = "36042";
            var process = Tools.TaosAdapterTools.NewTaosAdapter(port);
            Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
            try
            {
                var connStr =
                    $"protocol=WebSocket;host=127.0.0.1;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true";
                var builder = new ConnectionStringBuilder(connStr);
                using (var client = DbDriver.Open(builder))
                {
                    client.Exec("create database if not exists test_consume_reconnect");
                    client.Exec(
                        "create table if not exists test_consume_reconnect.t1 (ts timestamp, a int, b float, c binary(10))");
                    client.Exec(
                        "create topic if not exists topic_consume_reconnect as select * from test_consume_reconnect.t1");
                    client.Exec("insert into test_consume_reconnect.t1 values (now, 1, 1.1, 'abc')");
                }

                var cfg = new Dictionary<string, string>()
                {
                    { "td.connect.type", "WebSocket" },
                    { "group.id", "test" },
                    { "auto.offset.reset", "earliest" },
                    { "td.connect.ip", "127.0.0.1" },
                    { "td.connect.user", "root" },
                    { "td.connect.pass", "taosdata" },
                    { "td.connect.port", $"{port}" },
                    { "client.id", "test_tmq_c" },
                    { "enable.auto.commit", "false" },
                    { "msg.with.table.name", "true" },
                    { "useSSL", "false" },
                    { "ws.autoReconnect", "true" },
                    { "ws.reconnect.retry.count", "3" },
                    { "ws.reconnect.interval.ms", "2000" }
                };
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                consumer.Subscribe("topic_consume_reconnect");
                Tools.TaosAdapterTools.StopTaosAdapter(process);
                Task.Run(() =>
                {
                    Thread.Sleep(3000);
                    Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
                });
                var data = consumer.Consume(1000);
                Assert.NotNull(data);
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(process);
                process.Dispose();
            }
        }
    }
}