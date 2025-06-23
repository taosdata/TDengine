using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using TDengine.Driver;
using TDengine.Driver.Client;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        private Process NewTaosAdapter(string port)
        {
            string exec;
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                exec = "C:\\TDengine\\taosadapter.exe";
            }
            else
            {
                exec = "taosadapter";
            }

            ProcessStartInfo startInfo = new ProcessStartInfo(exec, $"--port {port}");
            Process process = new Process { StartInfo = startInfo };
            return process;
        }

        private async Task Start(Process process, string port)
        {
            process.Start();
            await WaitForStart(port);
        }

        private void Stop(Process process)
        {
            if (process.HasExited)
            {
                return;
            }

            process.Kill();
        }

        private async Task WaitForStart(string port)
        {
            HttpClient client = new HttpClient();
            string url = $"http://127.0.0.1:{port}/-/ping";
            bool success = await WaitForPingSuccess(client, url);
            if (!success)
            {
                throw new Exception("Failed to start taosadapter");
            }
        }

        static async Task<bool> WaitForPingSuccess(HttpClient client, string url)
        {
            bool success = false;
            int retryCount = 20;
            int retryDelayMs = 100;

            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    HttpResponseMessage response = await client.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        success = true;
                        break;
                    }
                }
                catch (Exception e)
                {
                    // ignored
                }

                await Task.Delay(retryDelayMs);
            }

            return success;
        }

        [Fact]
        public void QueryReconnect()
        {
            var port = "36043";
            var process = NewTaosAdapter(port);
            Start(process, port).Wait();
            Thread.Sleep(1000);
            var connStr =
                $"protocol=WebSocket;host=localhost;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true;autoReconnect=true;";

            var builder = new ConnectionStringBuilder(connStr);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    client.Exec("drop database if exists test_query_reconnect");
                    client.Exec("create database test_query_reconnect");
                    client.Exec("create table test_query_reconnect.t1 (ts timestamp, a int, b float, c binary(10))");
                    Stop(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Start(process, port).Wait();
                    });
                    client.Exec("insert into test_query_reconnect.t1 values (now, 1, 1.1, 'abc')");
                    using (var rows = client.Query("select * from test_query_reconnect.t1"))
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                    }

                    Stop(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Start(process, port).Wait();
                    });
                    using (var rows = client.Query("select * from test_query_reconnect.t1"))
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                    }
                }
                finally
                {
                    Stop(process);
                }
            }
        }

        [Fact]
        public void SchemalessReconnect()
        {
            var port = "36044";
            var process = NewTaosAdapter(port);
            try
            {
                Start(process, port).Wait();
                var connStr =
                    $"protocol=WebSocket;host=localhost;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true;autoReconnect=true;";

                var builder = new ConnectionStringBuilder(connStr);
                using (var client = DbDriver.Open(builder))
                {
                    client.Exec("drop database if exists test_sml_reconnect");
                    client.Exec("create database test_sml_reconnect");
                }

                var data = new string[]
                {
                    "sys_if_bytes_out 1479496100 1.3E3 host=web01 interface=eth0",
                    "sys_procs_running 1479496100 42 host=web01",
                };
                builder.Database = "test_sml_reconnect";
                using (var client = DbDriver.Open(builder))
                {
                    Stop(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Start(process, port).Wait();
                    });
                    client.SchemalessInsert(data, TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                }
            }
            finally
            {
                Stop(process);
            }
        }

        [Fact]
        public void StmtInitReconnect()
        {
            var port = "36045";
            var process = NewTaosAdapter(port);
            try
            {
                Start(process, port).Wait();
                var connStr =
                    $"protocol=WebSocket;host=localhost;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true;autoReconnect=true;";

                var builder = new ConnectionStringBuilder(connStr);
                using (var client = DbDriver.Open(builder))
                {
                    Stop(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Start(process, port).Wait();
                    });
                    client.StmtInit();
                }
            }
            finally
            {
                Stop(process);
            }
        }
    }
}