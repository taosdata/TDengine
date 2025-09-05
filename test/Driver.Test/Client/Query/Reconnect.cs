using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.Driver.Impl.WebSocketMethods;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using Xunit;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        [Fact]
        public void QueryReconnect()
        {
            var port = "36043";
            var process = Tools.TaosAdapterTools.NewTaosAdapter(port);
            Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
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
                    Tools.TaosAdapterTools.StopTaosAdapter(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
                    });
                    client.Exec("insert into test_query_reconnect.t1 values (now, 1, 1.1, 'abc')");
                    using (var rows = client.Query("select * from test_query_reconnect.t1"))
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                    }

                    Tools.TaosAdapterTools.StopTaosAdapter(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
                    });
                    using (var rows = client.Query("select * from test_query_reconnect.t1"))
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                    }
                }
                finally
                {
                    Tools.TaosAdapterTools.StopTaosAdapter(process);
                    process.Dispose();
                }
            }
        }

        [Fact]
        public void SchemalessReconnect()
        {
            var port = "36044";
            var process = Tools.TaosAdapterTools.NewTaosAdapter(port);
            try
            {
                Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
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
                    Tools.TaosAdapterTools.StopTaosAdapter(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
                    });
                    client.SchemalessInsert(data, TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                }
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(process);
                process.Dispose();
            }
        }

        [Fact]
        public void StmtInitReconnect()
        {
            var port = "36045";
            var process = Tools.TaosAdapterTools.NewTaosAdapter(port);
            try
            {
                Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
                var connStr =
                    $"protocol=WebSocket;host=localhost;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true;autoReconnect=true;";

                var builder = new ConnectionStringBuilder(connStr);
                using (var client = DbDriver.Open(builder))
                {
                    Tools.TaosAdapterTools.StopTaosAdapter(process);
                    Task.Run(() =>
                    {
                        Thread.Sleep(3000);
                        Tools.TaosAdapterTools.StartTaosAdapter(process, port).Wait();
                    });
                    client.StmtInit();
                }
            }
            finally
            {
                Tools.TaosAdapterTools.StopTaosAdapter(process);
                process.Dispose();
            }
        }

        [Fact]
        public void StmtPrepareReconnect()
        {
            var port = 36046;
            var prepareFail = true;
            var prepareClose = false;
            ulong stmtId = 0;
            var reconnectSuccessTest = false;
            var prepareClosedCount = 0;
            var fields = new List<Stmt2AllField>(1)
            {
                new Stmt2AllField
                {
                    Name = "ts",
                    FieldType = 9,
                    Precision = 0,
                    Scale = 0,
                    Bytes = 8,
                    BindType = 1
                }
            };
            Action<WebSocket, WebSocketMessageType, byte[]> messageHandler =
                (webSocket, messageType, message) =>
                {
                    _output.WriteLine(Encoding.UTF8.GetString(message));
                    var req = JsonConvert.DeserializeObject<WSActionReq<WSStmt2InitReq>>(
                        Encoding.UTF8.GetString(message));
                    switch (req.Action)
                    {
                        case "version":
                        {
                            var resp = new WSVersionResp
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                                Version = "3.3.6.0"
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                        case "conn":
                        {
                            var resp = new WSConnResp
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                        case "stmt2_init":
                        {
                            stmtId += 1;
                            var resp = new WSStmt2InitResp()
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                                StmtId = stmtId,
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                        case "stmt2_prepare":
                        {
                            if (prepareClose || (reconnectSuccessTest && prepareClosedCount == 0))
                            {
                                _output.WriteLine("prepare close connection");
                                var closeTask = Task.Run(async () =>
                                    await webSocket
                                        .CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None)
                                        .ConfigureAwait(false));
                                closeTask.Wait();
                                if (reconnectSuccessTest)
                                {
                                    prepareClosedCount += 1;
                                }
                                return;
                            }


                            var resp = new WSStmt2PrepareResp()
                            {
                                Code = prepareFail ? -1 : 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                                StmtId = 1,
                                IsInsert = true,
                                FieldsCount = 1,
                                Fields = fields,
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                    }
                };
            var mockServer = new MockWSServer(port, messageHandler);
            mockServer.Start();
            var connStr =
                $"protocol=WebSocket;host=localhost;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true;autoReconnect=true;";

            var builder = new ConnectionStringBuilder(connStr);
            using (var client = DbDriver.Open(builder))
            {
                using (var stmt = client.StmtInit())
                {
                    Assert.Throws<TDengineError>(() => stmt.Prepare("fail"));
                    prepareFail = false;
                    prepareClose = true;

                    Assert.Throws<TDengineError>(() => stmt.Prepare("close"));
                    prepareClose = false;
                    stmt.Prepare("success");
                    var colFields = stmt.GetColFields();
                    Assert.Single(colFields);
                    Assert.Equal(9, colFields[0].type);
                    // reconnect success
                    reconnectSuccessTest = true;
                    stmt.Prepare("reconnect success");
                    colFields = stmt.GetColFields();
                    Assert.Single(colFields);
                    Assert.Equal(9, colFields[0].type);
                }
            }

            mockServer.Dispose();
        }

        [Fact]
        public void StmtExecReconnect()
        {
            var port = 36047;
            var bindFail = false;
            var bindClose = false;
            var execFail = false;
            var execClose = false;
            var reconnectSuccessTest = false;
            var bindFailedCount = 0;
            var schemaChangedTest = false;
            ulong stmtId = 0;
            var fields = new List<Stmt2AllField>(1)
            {
                new Stmt2AllField
                {
                    Name = "ts",
                    FieldType = 9,
                    Precision = 0,
                    Scale = 0,
                    Bytes = 8,
                    BindType = 1
                }
            };
            var changedFields = new List<Stmt2AllField>(1)
            {
                new Stmt2AllField
                {
                    Name = "ts",
                    FieldType = 10,
                    Precision = 0,
                    Scale = 0,
                    Bytes = 8,
                    BindType = 1
                }
            };
            Action<WebSocket, WebSocketMessageType, byte[]> messageHandler =
                (webSocket, messageType, message) =>
                {
                    if (messageType == WebSocketMessageType.Binary)
                    {
                        _output.WriteLine("receive binary message");
                        var reqId = BitConverter.ToUInt64(message, 0);
                        var reqStmtId = BitConverter.ToUInt64(message, 8);
                        var action = BitConverter.ToUInt64(message, 16);
                        Assert.Equal((ulong)WSActionBinary.Stmt2BindMessage, action);

                        Assert.Equal(stmtId, reqStmtId);
                        if (bindClose || (reconnectSuccessTest && bindFailedCount == 0))
                        {
                            _output.WriteLine("bind close connection");
                            var closeTask = Task.Run(async () =>
                                await webSocket
                                    .CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None)
                                    .ConfigureAwait(false));
                            closeTask.Wait();
                            if (reconnectSuccessTest)
                            {
                                bindFailedCount += 1;
                            }
                            return;
                        }

                        var resp = new WSStmt2BindResp()
                        {
                            Code = bindFail ? -1 : 0,
                            Action = "stmt2_bind",
                            ReqId = reqId,
                            StmtId = reqStmtId,
                        };
                        var respStr = JsonConvert.SerializeObject(resp);
                        var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                        var task = Task.Run(async () => await webSocket
                            .SendAsync(data, WebSocketMessageType.Text, true, CancellationToken.None)
                            .ConfigureAwait(false));
                        task.Wait();
                        return;
                    }

                    _output.WriteLine(Encoding.UTF8.GetString(message));
                    var req = JsonConvert.DeserializeObject<WSActionReq<WSStmt2ExecReq>>(
                        Encoding.UTF8.GetString(message));
                    switch (req.Action)
                    {
                        case "version":
                        {
                            var resp = new WSVersionResp
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                                Version = "3.3.6.0"
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                        case "conn":
                        {
                            var resp = new WSConnResp
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                        case "stmt2_init":
                        {
                            stmtId += 1;
                            var resp = new WSStmt2InitResp()
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                                StmtId = stmtId,
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                        case "stmt2_prepare":
                        {
                            var resp = new WSStmt2PrepareResp()
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                                StmtId = 1,
                                IsInsert = true,
                                FieldsCount = 1,
                                Fields = schemaChangedTest?changedFields:fields,
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                        case "stmt2_exec":
                        {
                            Assert.Equal(stmtId, req.Args.StmtId);
                            if (execClose)
                            {
                                _output.WriteLine("exec close connection");
                                var closeTask = Task.Run(async () =>
                                    await webSocket
                                        .CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None)
                                        .ConfigureAwait(false));
                                closeTask.Wait();
                                return;
                            }

                            var resp = new WSStmt2ExecResp()
                            {
                                Code = execFail ? -1 : 0,
                                Action = req.Action,
                                ReqId = req.Args.ReqId,
                                StmtId = 1,
                                Affected = 1,
                            };
                            var respStr = JsonConvert.SerializeObject(resp);
                            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
                            var task = Task.Run(async () => await webSocket
                                .SendAsync(data, messageType, true, CancellationToken.None)
                                .ConfigureAwait(false));
                            task.Wait();
                            break;
                        }
                    }
                };
            var mockServer = new MockWSServer(port, messageHandler);
            mockServer.Start();
            var connStr =
                $"protocol=WebSocket;host=localhost;port={port};useSSL=false;username=root;password=taosdata;enableCompression=true;autoReconnect=true;";

            var builder = new ConnectionStringBuilder(connStr);
            using (var client = DbDriver.Open(builder))
            {
                using (var stmt = client.StmtInit())
                {
                    stmt.Prepare("success");
                    var colFields = stmt.GetColFields();
                    Assert.Single(colFields);
                    Assert.Equal(9, colFields[0].type);
                    // bind fail
                    stmt.BindRow(new object[] { (long)1 });
                    stmt.AddBatch();
                    bindFail = true;
                    Assert.Throws<TDengineError>(() => stmt.Exec());
                    // bind close
                    bindFail = false;
                    bindClose = true;
                    stmt.BindRow(new object[] { (long)1 });
                    stmt.AddBatch();
                    Assert.Throws<TDengineError>(() => stmt.Exec());
                    // bind success exec fail
                    bindClose = false;
                    execFail = true;
                    stmt.BindRow(new object[] { (long)1 });
                    stmt.AddBatch();
                    Assert.Throws<TDengineError>(() => stmt.Exec());
                    // bind success exec close
                    execFail = false;
                    execClose = true;
                    stmt.BindRow(new object[] { (long)1 });
                    stmt.AddBatch();
                    Assert.Throws<TDengineError>(() => stmt.Exec());
                    // bind success exec success
                    execClose = false;
                    stmt.BindRow(new object[]{(long)1});
                    stmt.AddBatch();
                    stmt.Exec();
                    Assert.Equal(1,stmt.Affected());
                    // reconnect success
                    reconnectSuccessTest = true;
                    stmt.BindRow(new object[] { (long)1 });
                    stmt.AddBatch();
                    stmt.Exec();
                    Assert.Equal(1, stmt.Affected());
                    reconnectSuccessTest = false;
                    // test schema changed
                    bindClose = true;
                    schemaChangedTest = true;
                    stmt.BindRow(new object[] { (long)1 });
                    stmt.AddBatch();
                    Assert.Throws<InvalidOperationException>(() => stmt.Exec());
                    Assert.Throws<InvalidOperationException>(() => stmt.BindRow(new object[] { (long)1 }));
                    bindClose = false;
                    schemaChangedTest = false;
                    stmt.Prepare("success");
                    stmt.BindRow(new object[] { (long)1 });
                    stmt.AddBatch();
                    stmt.Exec();
                    Assert.Equal(1, stmt.Affected());
                }
            }

            mockServer.Dispose();
        }
    }
}