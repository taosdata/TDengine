using System;
using System.Data;
using System.Diagnostics;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Driver.Test.Client.Query;
using Newtonsoft.Json;
using TDengine.Data.Client;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Data.Tests
{
    public class TDengineConnectionTests
    {
        private TDengineConnection _connection;
        private bool _is3360Test;
        private readonly ITestOutputHelper _output;


        public TDengineConnectionTests(ITestOutputHelper output)
        {
            _output = output;
            this._is3360Test = Environment.GetEnvironmentVariable("TD_3360_TEST") == "true";
            // this._is3360Test = true;
            _connection = new TDengineConnection("");
        }

        [Fact]
        public void ConnectionString_Property_Should_Set_ConnectionString_And_ConnectionStringBuilder()
        {
            // Arrange
            string connectionString = "username=root;password=taosdata";

            // Act
            _connection.ConnectionString = connectionString;

            // Assert
            Assert.Equal(connectionString, _connection.ConnectionString);
            Assert.NotNull(_connection.ConnectionStringBuilder);
            Assert.Equal(connectionString, _connection.ConnectionStringBuilder.ConnectionString);
        }

        [Fact]
        public void Database_Property_Should_Return_DatabaseName_From_ConnectionStringBuilder()
        {
            // Arrange
            string databaseName = "test";
            _connection.ConnectionStringBuilder = new TDengineConnectionStringBuilder($"db={databaseName}");

            // Act
            var result = _connection.Database;

            // Assert
            Assert.Equal(databaseName, result);
        }

        [Fact]
        public void State_Property_Should_Return_Initial_State_Closed()
        {
            // Arrange and Act
            var result = _connection.State;

            // Assert
            Assert.Equal(ConnectionState.Closed, result);
        }

        [Fact]
        public void DataSource_Property_Should_Return_Host_From_ConnectionStringBuilder()
        {
            // Arrange
            string host = "localhost";
            _connection.ConnectionStringBuilder = new TDengineConnectionStringBuilder($"Host={host}");

            // Act
            var result = _connection.DataSource;

            // Assert
            Assert.Equal(host, result);
        }

        [Fact]
        public void Close_Method_Should_Close_Connection()
        {
            // Arrange
            _connection.ConnectionString = "username=root;password=taosdata";
            _connection.Open();

            // Act
            _connection.Close();

            // Assert
            Assert.Equal(ConnectionState.Closed, _connection.State);
        }

        [Fact]
        public void TestChangeDatabase()
        {
            var connection = new TDengineConnection("username=root;password=taosdata");
            Assert.Equal("", connection.Database);
            connection = new TDengineConnection("username=root;password=taosdata;db=test_db");
            Assert.Equal("test_db", connection.Database);
            connection = new TDengineConnection("username=root;password=taosdata");
            connection.Open();
            Assert.Equal("", connection.Database);
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "create database if not exists test_db";
                cmd.ExecuteNonQuery();
            }

            connection.ChangeDatabase("test_db");
            Assert.Equal("test_db", connection.Database);
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "drop database if exists test_db";
                cmd.ExecuteNonQuery();
            }

            connection.Close();
        }

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
                catch (Exception)
                {
                    // ignored
                }

                await Task.Delay(retryDelayMs);
            }

            return success;
        }

        [Fact]
        public void TestState()
        {
            var port = 56041;

            void MessageHandler(WebSocket webSocket, WebSocketMessageType messageType, byte[] message)
            {
                _output.WriteLine(Encoding.UTF8.GetString(message));
                var req = JsonConvert.DeserializeObject<WSActionReq<WSStmt2InitReq>>(Encoding.UTF8.GetString(message));
                switch (req.Action)
                {
                    case "version":
                    {
                        var resp = new WSVersionResp { Code = 0, Action = req.Action, ReqId = req.Args.ReqId, Version = "3.3.6.0" };
                        var respStr = JsonConvert.SerializeObject(resp);
                        var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                        var task = Task.Run(async () => await webSocket.SendAsync(data, messageType, true, CancellationToken.None)
                            .ConfigureAwait(false));
                        task.Wait();
                        break;
                    }
                    case "conn":
                    {
                        var resp = new WSConnResp { Code = 0, Action = req.Action, ReqId = req.Args.ReqId, };
                        var respStr = JsonConvert.SerializeObject(resp);
                        var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                        var task = Task.Run(async () => await webSocket.SendAsync(data, messageType, true, CancellationToken.None)
                            .ConfigureAwait(false));
                        task.Wait();
                        break;
                    }
                }
            }

            var mockServer = new MockWSServer(port, MessageHandler);
            mockServer.Start();
            var connStr =
                $"protocol=WebSocket;host=localhost;port={port};useSSL=false;username=root;password=taosdata;";
            var connection = new TDengineConnection(connStr);
            Assert.Equal(ConnectionState.Closed, connection.State);
            connection.Open();
            Assert.Equal(ConnectionState.Open, connection.State);
            connection.Close();
            Assert.Equal(ConnectionState.Closed, connection.State);
            connection.Open();
            Assert.Equal(ConnectionState.Open, connection.State);
            mockServer.Dispose();
            Thread.Sleep(1000);
            Assert.Equal(ConnectionState.Broken, connection.State);
            connection.Close();
            connection.Dispose();
        }

        [Fact]
        public void TestServerVersion()
        {
            var connection = new TDengineConnection("username=root;password=taosdata");
            Assert.Equal("", connection.ServerVersion);
            connection.Open();
            var serverVersion = connection.ServerVersion;
            if (_is3360Test)
            {
                Assert.Equal("3.3.6.0", serverVersion);
            }
            else
            {
                Assert.NotNull(serverVersion);
                Assert.NotEmpty(serverVersion);
            }

            connection.Close();
        }
    }
}