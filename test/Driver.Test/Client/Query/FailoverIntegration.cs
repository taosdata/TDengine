using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using TDengine.Driver;
using TDengine.Driver.Client;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        [Fact]
        public void MultiAddressConnectShouldFailoverToSecondRealAdapterWhenFirstUnavailable()
        {
            var unavailablePort = GetFreePort();
            var availablePort = GetFreePort();
            while (availablePort == unavailablePort)
            {
                availablePort = GetFreePort();
            }

            Process availableProcess = null;
            try
            {
                availableProcess = Tools.TaosAdapterTools.NewTaosAdapter(availablePort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(availableProcess, availablePort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{unavailablePort},localhost:{availablePort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=200;" +
                              "reconnectRetryCount=10;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var rows = client.Query("select server_version()"))
                    {
                        Assert.True(rows.Read());
                    }
                }
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(availableProcess);
                availableProcess?.Dispose();
            }
        }

        [Fact]
        public void MultiAddressIpv6ConnectShouldFailoverToSecondRealAdapterWhenFirstUnavailable()
        {
            if (_is3360Test)
            {
                _output.WriteLine("Skipping IPv6 failover integration on 3.3.6.0 because taosadapter 3.3.6.0 only supports IPv4.");
                return;
            }

            var unavailablePort = GetFreePort();
            var availablePort = GetFreePort();
            while (availablePort == unavailablePort)
            {
                availablePort = GetFreePort();
            }

            Process availableProcess = null;
            try
            {
                availableProcess = Tools.TaosAdapterTools.NewTaosAdapter(availablePort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(availableProcess, availablePort.ToString()).Wait();
                var canReachIpv6Loopback = Tools.TaosAdapterTools.CanPingHost("::1", availablePort.ToString())
                    .GetAwaiter()
                    .GetResult();
                Assert.True(canReachIpv6Loopback,
                    $"taosadapter on port {availablePort} should be reachable via IPv6 loopback.");

                var connStr = "protocol=WebSocket;" +
                              $"host=[::1]:{unavailablePort},[::1]:{availablePort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=200;" +
                              "reconnectRetryCount=10;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var rows = client.Query("select server_version()"))
                    {
                        Assert.True(rows.Read());
                    }
                }
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(availableProcess);
                availableProcess?.Dispose();
            }
        }

        [Fact]
        public void MultiAddressReconnectShouldFailoverToSecondRealAdapterWhenFirstStops()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            Process firstProcess = null;
            Process secondProcess = null;
            try
            {
                firstProcess = Tools.TaosAdapterTools.NewTaosAdapter(firstPort.ToString());
                secondProcess = Tools.TaosAdapterTools.NewTaosAdapter(secondPort.ToString());

                // Start first adapter only, so initial connection must be on first address.
                Tools.TaosAdapterTools.StartTaosAdapter(firstProcess, firstPort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=200;" +
                              "reconnectRetryCount=15;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var rows = client.Query("select server_version()"))
                    {
                        Assert.True(rows.Read());
                    }

                    Tools.TaosAdapterTools.StartTaosAdapter(secondProcess, secondPort.ToString()).Wait();
                    Thread.Sleep(300);

                    Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                    firstProcess = null;
                    Thread.Sleep(300);

                    using (var rows = client.Query("select server_version()"))
                    {
                        Assert.True(rows.Read());
                    }
                }
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
        public void MultiAddressExecShouldFailoverToSecondRealAdapterWhenFirstUnavailable()
        {
            var unavailablePort = GetFreePort();
            var availablePort = GetFreePort();
            while (availablePort == unavailablePort)
            {
                availablePort = GetFreePort();
            }

            Process availableProcess = null;
            const string preferredDb = "test_failover_exec";
            const string table = "t_failover_exec";
            try
            {
                availableProcess = Tools.TaosAdapterTools.NewTaosAdapter(availablePort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(availableProcess, availablePort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{unavailablePort},localhost:{availablePort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=200;" +
                              "reconnectRetryCount=10;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    var db = EnsureDatabase(client, preferredDb);
                    var marker = (int)(DateTime.UtcNow.Ticks % int.MaxValue);
                    try
                    {
                        client.Exec($"create table if not exists {db}.{table} (ts timestamp, v int)");
                        var affected = client.Exec($"insert into {db}.{table} values (now, {marker})");
                        Assert.True(affected > 0);
                        using (var rows = client.Query($"select * from {db}.{table} where v = {marker} limit 1"))
                        {
                            Assert.True(rows.Read());
                        }
                    }
                    finally
                    {
                        client.Exec($"drop table if exists {db}.{table}");
                    }
                }
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(availableProcess);
                availableProcess?.Dispose();
            }
        }

        [Fact]
        public void MultiAddressSchemalessShouldFailoverToSecondRealAdapterWhenFirstUnavailable()
        {
            var unavailablePort = GetFreePort();
            var availablePort = GetFreePort();
            while (availablePort == unavailablePort)
            {
                availablePort = GetFreePort();
            }

            Process availableProcess = null;
            const string preferredDb = "test_failover_sml";
            try
            {
                availableProcess = Tools.TaosAdapterTools.NewTaosAdapter(availablePort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(availableProcess, availablePort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{unavailablePort},localhost:{availablePort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=200;" +
                              "reconnectRetryCount=10;" +
                              "connTimeout=00:00:03;";

                var builder = new ConnectionStringBuilder(connStr);
                using (var admin = DbDriver.Open(builder))
                {
                    var db = EnsureDatabase(admin, preferredDb);
                    builder.Database = db;
                }

                using (var client = DbDriver.Open(builder))
                {
                    var data = new[]
                    {
                        "sys_if_bytes_out 1479496100 1.3E3 host=web01 interface=eth0",
                        "sys_procs_running 1479496100 42 host=web01",
                    };
                    client.SchemalessInsert(data, TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                    using (var rows = client.Query("show tables"))
                    {
                        Assert.True(rows.Read());
                    }
                }
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(availableProcess);
                availableProcess?.Dispose();
            }
        }

        [Fact]
        public void ConcurrentQueryAndDisposeShouldCompleteWithoutDeadlock()
        {
            var port = GetFreePort();
            Process process = null;
            ITDengineClient client = null;
            try
            {
                process = Tools.TaosAdapterTools.NewTaosAdapter(port.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(process, port.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{port};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=100;" +
                              "reconnectRetryCount=3;" +
                              "connTimeout=00:00:03;";

                client = DbDriver.Open(new ConnectionStringBuilder(connStr));
                var errors = new ConcurrentQueue<Exception>();
                var running = 1;
                var queryTask = Task.Run(() =>
                {
                    while (Volatile.Read(ref running) == 1)
                    {
                        try
                        {
                            QueryServerVersion(client);
                        }
                        catch (Exception e)
                        {
                            errors.Enqueue(e);
                            Thread.Sleep(5);
                        }
                    }
                });

                Thread.Sleep(200);
                var disposeTask = Task.Run(() => client.Dispose());
                Assert.True(disposeTask.Wait(TimeSpan.FromSeconds(5)), "dispose should complete within timeout");
                Interlocked.Exchange(ref running, 0);
                Assert.True(queryTask.Wait(TimeSpan.FromSeconds(5)), "query task should exit within timeout");
                Assert.DoesNotContain(errors, e => e is TimeoutException);
            }
            finally
            {
                client?.Dispose();
                Tools.TaosAdapterTools.StopTaosAdapter(process);
                process?.Dispose();
            }
        }

        [Fact]
        public void AutoReconnectFalseShouldNotFailoverWhenPrimaryStops()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            Process firstProcess = null;
            Process secondProcess = null;
            try
            {
                firstProcess = Tools.TaosAdapterTools.NewTaosAdapter(firstPort.ToString());
                secondProcess = Tools.TaosAdapterTools.NewTaosAdapter(secondPort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(firstProcess, firstPort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=false;" +
                              "reconnectIntervalMs=100;" +
                              "reconnectRetryCount=10;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    QueryServerVersion(client);
                    Tools.TaosAdapterTools.StartTaosAdapter(secondProcess, secondPort.ToString()).Wait();
                    Thread.Sleep(300);
                    Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                    firstProcess = null;
                    Thread.Sleep(300);
                    Assert.ThrowsAny<Exception>(() => QueryServerVersion(client));
                }
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
        public void ReconnectRetryCountZeroShouldNotFailoverWhenPrimaryStops()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            Process firstProcess = null;
            Process secondProcess = null;
            try
            {
                firstProcess = Tools.TaosAdapterTools.NewTaosAdapter(firstPort.ToString());
                secondProcess = Tools.TaosAdapterTools.NewTaosAdapter(secondPort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(firstProcess, firstPort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=100;" +
                              "reconnectRetryCount=0;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    QueryServerVersion(client);
                    Tools.TaosAdapterTools.StartTaosAdapter(secondProcess, secondPort.ToString()).Wait();
                    Thread.Sleep(300);
                    Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                    firstProcess = null;
                    Thread.Sleep(300);
                    Assert.ThrowsAny<Exception>(() => QueryServerVersion(client));
                }
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
        public void ReconnectIntervalZeroShouldStillFailoverWhenPrimaryStops()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            Process firstProcess = null;
            Process secondProcess = null;
            try
            {
                firstProcess = Tools.TaosAdapterTools.NewTaosAdapter(firstPort.ToString());
                secondProcess = Tools.TaosAdapterTools.NewTaosAdapter(secondPort.ToString());
                Tools.TaosAdapterTools.StartTaosAdapter(firstProcess, firstPort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=0;" +
                              "reconnectRetryCount=10;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    QueryServerVersion(client);
                    Tools.TaosAdapterTools.StartTaosAdapter(secondProcess, secondPort.ToString()).Wait();
                    Thread.Sleep(300);
                    Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                    firstProcess = null;
                    Thread.Sleep(300);
                    QueryServerVersion(client);
                }
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
        public void MultiAddressReconnectShouldSurviveMultipleRoundsAcrossThreeAdapters()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            var thirdPort = GetFreePort();
            while (thirdPort == firstPort || thirdPort == secondPort)
            {
                thirdPort = GetFreePort();
            }

            Process firstProcess = null;
            Process secondProcess = null;
            Process thirdProcess = null;
            try
            {
                firstProcess = Tools.TaosAdapterTools.NewTaosAdapter(firstPort.ToString());
                secondProcess = Tools.TaosAdapterTools.NewTaosAdapter(secondPort.ToString());
                thirdProcess = Tools.TaosAdapterTools.NewTaosAdapter(thirdPort.ToString());

                Tools.TaosAdapterTools.StartTaosAdapter(firstProcess, firstPort.ToString()).Wait();
                Tools.TaosAdapterTools.StartTaosAdapter(secondProcess, secondPort.ToString()).Wait();
                Tools.TaosAdapterTools.StartTaosAdapter(thirdProcess, thirdPort.ToString()).Wait();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort},localhost:{thirdPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectIntervalMs=100;" +
                              "reconnectRetryCount=5;" +
                              "connTimeout=00:00:03;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    QueryServerVersion(client);

                    var processes = new[] { firstProcess, secondProcess, thirdProcess };
                    var ports = new[] { firstPort, secondPort, thirdPort };
                    var running = new[] { true, true, true };
                    var seed = GetPositiveIntFromEnvironment("WS_FAILOVER_RANDOM_SEED",
                        (int)(DateTime.UtcNow.Ticks & int.MaxValue));
                    var rounds = GetPositiveIntFromEnvironment("WS_FAILOVER_RANDOM_ROUNDS", 10);
                    var random = new Random(seed);
                    _output.WriteLine(
                        $"MultiAddressReconnect random rounds: seed={seed}, rounds={rounds}, ports={firstPort},{secondPort},{thirdPort}");

                    for (var round = 0; round < rounds; round++)
                    {
                        for (var i = 0; i < processes.Length; i++)
                        {
                            if (running[i])
                            {
                                continue;
                            }

                            Tools.TaosAdapterTools.StartTaosAdapter(processes[i], ports[i].ToString()).Wait();
                            running[i] = true;
                        }

                        var stopCount = random.Next(1, 4);
                        var order = new[] { 0, 1, 2 };
                        for (var i = order.Length - 1; i > 0; i--)
                        {
                            var j = random.Next(i + 1);
                            var tmp = order[i];
                            order[i] = order[j];
                            order[j] = tmp;
                        }

                        for (var i = 0; i < stopCount; i++)
                        {
                            var idx = order[i];
                            if (!running[idx])
                            {
                                continue;
                            }

                            Tools.TaosAdapterTools.StopTaosAdapter(processes[idx]);
                            running[idx] = false;
                        }

                        Thread.Sleep(300);
                        var aliveCount = 0;
                        for (var i = 0; i < running.Length; i++)
                        {
                            if (running[i])
                            {
                                aliveCount++;
                            }
                        }

                        if (aliveCount > 0)
                        {
                            QueryServerVersion(client);
                            continue;
                        }

                        Assert.ThrowsAny<Exception>(() => QueryServerVersion(client));
                        var restartIndex = random.Next(0, processes.Length);
                        Tools.TaosAdapterTools.StartTaosAdapter(processes[restartIndex], ports[restartIndex].ToString())
                            .Wait();
                        running[restartIndex] = true;
                        Thread.Sleep(300);
                        QueryServerVersion(client);
                        Tools.TaosAdapterTools.StopTaosAdapter(processes[restartIndex]);
                        running[restartIndex] = false;
                    }
                }
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(firstProcess);
                firstProcess?.Dispose();
                Tools.TaosAdapterTools.StopTaosAdapter(secondProcess);
                secondProcess?.Dispose();
                Tools.TaosAdapterTools.StopTaosAdapter(thirdProcess);
                thirdProcess?.Dispose();
            }
        }

        private static int GetPositiveIntFromEnvironment(string key, int defaultValue)
        {
            var raw = Environment.GetEnvironmentVariable(key);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return defaultValue;
            }

            if (!int.TryParse(raw, out var parsed))
            {
                return defaultValue;
            }

            return parsed > 0 ? parsed : defaultValue;
        }

        private static void QueryServerVersion(ITDengineClient client)
        {
            using (var rows = client.Query("select server_version()"))
            {
                Assert.True(rows.Read());
            }
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

        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var endpoint = (IPEndPoint)listener.LocalEndpoint;
            listener.Stop();
            return endpoint.Port;
        }
    }
}
