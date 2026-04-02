using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
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
        public void MultiAddressSubscribeShouldFailoverToSecondAdapterWhenFirstUnavailable()
        {
            var unavailablePort = GetFreePort();
            var availablePort = GetFreePort();
            while (availablePort == unavailablePort)
            {
                availablePort = GetFreePort();
            }

            Process availableProcess = null;
            const string preferredDb = "test_tmq_failover";
            const string topic = "topic_tmq_failover_sub";
            try
            {
                availableProcess = Tools.TaosAdapterTools.NewTaosAdapter(availablePort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(availableProcess, availablePort.ToString()).Wait();

                var availableConnStr = BuildWsConnectionString(availablePort);
                using (var admin = DbDriver.Open(new ConnectionStringBuilder(availableConnStr)))
                {
                    admin.Exec($"drop topic if exists {topic}");
                    var db = EnsureDatabase(admin, preferredDb);
                    admin.Exec($"create table if not exists {db}.t (ts timestamp, a int)");
                    admin.Exec($"create topic if not exists {topic} as select * from {db}.t");
                    admin.Exec($"insert into {db}.t values (now, 1)");
                }

                var cfg = BuildWsFailoverConfig(unavailablePort, availablePort,
                    $"test_tmq_failover_group_{Guid.NewGuid().ToString("N")}");
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe(topic);
                    ConsumeResult<Dictionary<string, object>> result = null;
                    for (var i = 0; i < 5; i++)
                    {
                        result = consumer.Consume(1000);
                        if (result != null)
                        {
                            break;
                        }
                    }

                    Assert.NotNull(result);
                }
                finally
                {
                    try
                    {
                        consumer.Unsubscribe();
                    }
                    catch
                    {
                    }

                    consumer.Close();
                }

                DropTopicWithRetry(availableConnStr, topic);
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(availableProcess);
                availableProcess?.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConsumeShouldReconnectToSecondAdapterWhenFirstStops()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            Process firstProcess = null;
            Process secondProcess = null;
            const string preferredDb = "test_tmq_failover";
            const string topic = "topic_tmq_failover_consume";
            var db = preferredDb;
            try
            {
                firstProcess = Tools.TaosAdapterTools.NewTaosAdapter(firstPort.ToString());
                secondProcess = Tools.TaosAdapterTools.NewTaosAdapter(secondPort.ToString());

                Tools.TaosAdapterTools.StartTaosAdapter(firstProcess, firstPort.ToString()).Wait();
                var firstConnStr = BuildWsConnectionString(firstPort);
                using (var admin = DbDriver.Open(new ConnectionStringBuilder(firstConnStr)))
                {
                    admin.Exec($"drop topic if exists {topic}");
                    db = EnsureDatabase(admin, preferredDb);
                    admin.Exec($"create table if not exists {db}.t (ts timestamp, a int)");
                    admin.Exec($"create topic if not exists {topic} as select * from {db}.t");
                    admin.Exec($"insert into {db}.t values (now, 1)");
                }

                var cfg = BuildWsFailoverConfig(firstPort, secondPort,
                    $"test_tmq_failover_group_{Guid.NewGuid().ToString("N")}");
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe(topic);
                    for (var i = 0; i < 5; i++)
                    {
                        var initial = consumer.Consume(1000);
                        if (initial != null)
                        {
                            break;
                        }
                    }

                    Tools.TaosAdapterTools.StartTaosAdapter(secondProcess, secondPort.ToString()).Wait();
                    Thread.Sleep(300);
                    Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                    Thread.Sleep(300);

                    var secondConnStr = BuildWsConnectionString(secondPort);
                    using (var secondAdmin = DbDriver.Open(new ConnectionStringBuilder(secondConnStr)))
                    {
                        secondAdmin.Exec($"insert into {db}.t values (now, 2)");
                        ConsumeResult<Dictionary<string, object>> result = null;
                        for (var i = 0; i < 20; i++)
                        {
                            result = consumer.Consume(1000);
                            if (result != null)
                            {
                                break;
                            }

                            secondAdmin.Exec($"insert into {db}.t values (now, {i + 3})");
                        }

                        Assert.NotNull(result);
                    }
                }
                finally
                {
                    try
                    {
                        consumer.Unsubscribe();
                    }
                    catch
                    {
                    }

                    consumer.Close();
                }

                var cleanupConnStr = BuildWsConnectionString(secondPort);
                DropTopicWithRetry(cleanupConnStr, topic);
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                firstProcess?.Dispose();
                Tools.TaosAdapterTools.StopTaosAdapter(secondProcess);
                secondProcess?.Dispose();
            }
        }

        [Fact]
        public void ConcurrentConsumeAndCloseShouldCompleteWithoutDeadlock()
        {
            var port = GetFreePort();
            Process process = null;
            const string preferredDb = "test_tmq_close_concurrency";
            const string topic = "topic_tmq_close_concurrency";
            IConsumer<Dictionary<string, object>> consumer = null;
            try
            {
                process = Tools.TaosAdapterTools.NewTaosAdapter(port.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(process, port.ToString()).Wait();
                var connStr = BuildWsConnectionString(port);
                using (var admin = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    admin.Exec($"drop topic if exists {topic}");
                    var db = EnsureDatabase(admin, preferredDb);
                    admin.Exec($"create table if not exists {db}.t_close (ts timestamp, a int)");
                    admin.Exec($"create topic if not exists {topic} as select * from {db}.t_close");
                    for (var i = 0; i < 10; i++)
                    {
                        admin.Exec($"insert into {db}.t_close values (now + {i}a, {i})");
                    }
                }

                var cfg = BuildWsFailoverConfig(port, GetFreePort(),
                    $"test_tmq_close_group_{Guid.NewGuid().ToString("N")}");
                consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                consumer.Subscribe(topic);

                var errors = new ConcurrentQueue<Exception>();
                var consumeTask = Task.Run(() =>
                {
                    try
                    {
                        for (var i = 0; i < 20; i++)
                        {
                            consumer.Consume(200);
                        }
                    }
                    catch (Exception e)
                    {
                        errors.Enqueue(e);
                    }
                });

                Thread.Sleep(100);
                var closeTask = Task.Run(() => consumer.Close());
                Assert.True(closeTask.Wait(TimeSpan.FromSeconds(5)), "close should complete within timeout");
                Assert.True(consumeTask.Wait(TimeSpan.FromSeconds(5)), "consume task should complete within timeout");
                Assert.DoesNotContain(errors, e => e is TimeoutException);
            }
            finally
            {
                try
                {
                    consumer?.Close();
                }
                catch
                {
                }

                try
                {
                    DropTopicWithRetry(BuildWsConnectionString(port), topic);
                }
                catch
                {
                }

                Tools.TaosAdapterTools.StopTaosAdapter(process);
                process?.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConsumeCommitShouldRemainConsistentAfterFailover()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            Process firstProcess = null;
            Process secondProcess = null;
            const string preferredDb = "test_tmq_failover_commit";
            const string topic = "topic_tmq_failover_commit";
            var db = preferredDb;
            var marker = (int)(DateTime.UtcNow.Ticks % 1000000) + 1000;
            try
            {
                firstProcess = Tools.TaosAdapterTools.NewTaosAdapter(firstPort.ToString());
                secondProcess = Tools.TaosAdapterTools.NewTaosAdapter(secondPort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(firstProcess, firstPort.ToString()).Wait();

                var firstConnStr = BuildWsConnectionString(firstPort);
                using (var admin = DbDriver.Open(new ConnectionStringBuilder(firstConnStr)))
                {
                    admin.Exec($"drop topic if exists {topic}");
                    db = EnsureDatabase(admin, preferredDb);
                    admin.Exec($"create table if not exists {db}.t_commit (ts timestamp, a int)");
                    admin.Exec($"create topic if not exists {topic} as select * from {db}.t_commit");
                    admin.Exec($"insert into {db}.t_commit values (now, 1)");
                    admin.Exec($"insert into {db}.t_commit values (now + 1a, 2)");
                }

                var cfg = BuildWsFailoverConfig(firstPort, secondPort,
                    $"test_tmq_failover_commit_group_{Guid.NewGuid().ToString("N")}");
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe(topic);
                    ConsumeResult<Dictionary<string, object>> firstResult = null;
                    for (var i = 0; i < 20; i++)
                    {
                        var result = consumer.Consume(1000);
                        if (result == null || result.Message.Count == 0)
                        {
                            continue;
                        }

                        firstResult = result;
                        break;
                    }

                    Assert.NotNull(firstResult);
                    consumer.Commit(new[] { firstResult.TopicPartitionOffset });
                    var committedBeforeFailover = consumer.Committed(new[] { firstResult.TopicPartition }, TimeSpan.Zero);
                    Assert.Single(committedBeforeFailover);
                    Assert.Equal(firstResult.Offset, committedBeforeFailover[0].Offset);

                    Tools.TaosAdapterTools.StartTaosAdapter(secondProcess, secondPort.ToString()).Wait();
                    Thread.Sleep(300);
                    Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                    Thread.Sleep(300);

                    var secondConnStr = BuildWsConnectionString(secondPort);
                    ConsumeResult<Dictionary<string, object>> markerResult = null;
                    using (var secondAdmin = DbDriver.Open(new ConnectionStringBuilder(secondConnStr)))
                    {
                        secondAdmin.Exec($"insert into {db}.t_commit values (now + 2a, {marker})");
                        for (var i = 0; i < 30; i++)
                        {
                            var result = consumer.Consume(1000);
                            if (result != null && ContainsValue(result, marker))
                            {
                                markerResult = result;
                                break;
                            }

                            secondAdmin.Exec($"insert into {db}.t_commit values (now + {i + 3}a, {marker + i + 1})");
                        }
                    }

                    Assert.NotNull(markerResult);
                    Assert.True(markerResult.Offset >= firstResult.Offset);
                    consumer.Commit(new[] { markerResult.TopicPartitionOffset });
                    var committedAfterFailover = consumer.Committed(new[] { markerResult.TopicPartition }, TimeSpan.Zero);
                    Assert.Single(committedAfterFailover);
                    Assert.Equal(markerResult.Offset, committedAfterFailover[0].Offset);
                }
                finally
                {
                    try
                    {
                        consumer.Unsubscribe();
                    }
                    catch
                    {
                    }

                    consumer.Close();
                }

                DropTopicWithRetry(BuildWsConnectionString(secondPort), topic);
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                firstProcess?.Dispose();
                Tools.TaosAdapterTools.StopTaosAdapter(secondProcess);
                secondProcess?.Dispose();
            }
        }

        private static string BuildWsConnectionString(int port)
        {
            return "protocol=WebSocket;" +
                   "host=127.0.0.1;" +
                   $"port={port};" +
                   "useSSL=false;" +
                   "username=root;" +
                   "password=taosdata;" +
                   "enableCompression=true";
        }

        private static Dictionary<string, string> BuildWsFailoverConfig(int firstPort, int secondPort, string groupId)
        {
            return new Dictionary<string, string>
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", groupId },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", $"127.0.0.1:{firstPort},127.0.0.1:{secondPort}" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "client.id", $"tmq_failover_client_{Guid.NewGuid().ToString("N")}" },
                { "enable.auto.commit", "false" },
                { "msg.with.table.name", "true" },
                { "useSSL", "false" },
                { "ws.autoReconnect", "true" },
                { "ws.reconnect.retry.count", "10" },
                { "ws.reconnect.interval.ms", "200" }
            };
        }

        private static bool ContainsValue(ConsumeResult<Dictionary<string, object>> result, int expected)
        {
            if (result == null || result.Message == null)
            {
                return false;
            }

            for (var i = 0; i < result.Message.Count; i++)
            {
                var value = result.Message[i].Value;
                if (value == null)
                {
                    continue;
                }

                if (!value.TryGetValue("a", out var rawValue))
                {
                    continue;
                }

                try
                {
                    if (Convert.ToInt32(rawValue) == expected)
                    {
                        return true;
                    }
                }
                catch
                {
                    // ignored
                }
            }

            return false;
        }

        private static void DropTopicWithRetry(string connectionString, string topic)
        {
            const int maxAttempts = 60;
            const int retryDelayMs = 500;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                try
                {
                    using (var admin = DbDriver.Open(new ConnectionStringBuilder(connectionString)))
                    {
                        admin.Exec($"drop topic if exists {topic}");
                    }

                    return;
                }
                catch (TDengineError e)
                {
                    if (e.Code != 0x3eb)
                    {
                        throw;
                    }

                    if (attempt == maxAttempts - 1)
                    {
                        // best effort cleanup: topic release may lag in server side for a while
                        return;
                    }

                    Thread.Sleep(retryDelayMs);
                }
            }
        }

        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var endpoint = (IPEndPoint)listener.LocalEndpoint;
            listener.Stop();
            return endpoint.Port;
        }

        private static string EnsureDatabase(ITDengineClient client, string preferredDb)
        {
            try
            {
                client.Exec($"create database if not exists {preferredDb}");
                return preferredDb;
            }
            catch (TDengineError e)
            {
                if (e.Code != 0x3b1)
                {
                    throw;
                }

                using (var rows = client.Query("select name from information_schema.ins_databases"))
                {
                    while (rows.Read())
                    {
                        var db = rows.GetString(0);
                        if (string.Equals(db, "information_schema", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        if (string.Equals(db, "performance_schema", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        return db;
                    }
                }

                throw;
            }
        }
    }
}
