using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using Xunit;

namespace Driver.Test.Client.Query
{
    public class Failover
    {
        [Fact]
        public void MultiAddressConnectShouldSelectLeastConnectionAddress()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }
            var firstConnCount = 0;
            var secondConnCount = 0;

            var firstServer = new MockWSServer(firstPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref firstConnCount); }));
            var secondServer = new MockWSServer(secondPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref secondConnCount); }));
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort},127.0.0.1:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:05;";

                using (var firstClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var secondClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                    {
                        Assert.Equal(1, Volatile.Read(ref firstConnCount));
                        Assert.Equal(1, Volatile.Read(ref secondConnCount));
                        Assert.True(firstClient.ConnectionAvailable());
                        Assert.True(secondClient.ConnectionAvailable());
                    }
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConnectShouldDistributeEvenlyUnderHighConcurrency()
        {
            const int addressCount = 2;
            const int clientCount = 20;
            var ports = new List<int>(addressCount);
            while (ports.Count < addressCount)
            {
                var candidate = GetFreePort();
                if (ports.Contains(candidate))
                {
                    continue;
                }

                ports.Add(candidate);
            }

            var connCounts = new int[addressCount];
            var servers = new List<MockWSServer>(addressCount);
            for (var i = 0; i < addressCount; i++)
            {
                ResetFailoverCacheConnectionCount(BuildWsCacheKey(ports[i]));
                var index = i;
                servers.Add(new MockWSServer(ports[i],
                    CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref connCounts[index]); })));
            }

            var clients = new ConcurrentBag<ITDengineClient>();
            var errors = new ConcurrentQueue<Exception>();
            var startGate = new ManualResetEventSlim(false);
            try
            {
                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Start();
                }

                Thread.Sleep(200);

                var hostList = string.Join(",", ports.Select(p => $"127.0.0.1:{p}"));
                var connStr = "protocol=WebSocket;" +
                              $"host={hostList};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:05;";

                var tasks = new Task[clientCount];
                for (var i = 0; i < clientCount; i++)
                {
                    tasks[i] = Task.Run(() =>
                    {
                        startGate.Wait();
                        Exception lastException = null;
                        for (var attempt = 0; attempt < 3; attempt++)
                        {
                            try
                            {
                                var client = DbDriver.Open(new ConnectionStringBuilder(connStr));
                                clients.Add(client);
                                return;
                            }
                            catch (Exception ex)
                            {
                                lastException = ex;
                                Thread.Sleep(30);
                            }
                        }

                        if (lastException != null)
                        {
                            errors.Enqueue(lastException);
                        }
                    });
                }

                startGate.Set();
                Task.WaitAll(tasks);

                Assert.Empty(errors);
                Assert.Equal(clientCount, connCounts.Sum());
                var minCount = connCounts.Min();
                var maxCount = connCounts.Max();
                var allowedSkew = Math.Max(2, clientCount / 5);
                Assert.True(maxCount - minCount <= allowedSkew,
                    $"connection distribution is not balanced under concurrency pressure (allowed skew: {allowedSkew}): [{string.Join(",", connCounts)}]");
            }
            finally
            {
                while (clients.TryTake(out var client))
                {
                    client.Dispose();
                }

                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Dispose();
                }

                startGate.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConnectShouldDistributeStrictlyUnderModerateConcurrency()
        {
            const int addressCount = 2;
            const int clientCount = 10;
            var ports = new List<int>(addressCount);
            while (ports.Count < addressCount)
            {
                var candidate = GetFreePort();
                if (ports.Contains(candidate))
                {
                    continue;
                }

                ports.Add(candidate);
            }

            var connCounts = new int[addressCount];
            var servers = new List<MockWSServer>(addressCount);
            for (var i = 0; i < addressCount; i++)
            {
                ResetFailoverCacheConnectionCount(BuildWsCacheKey(ports[i]));
                var index = i;
                servers.Add(new MockWSServer(ports[i],
                    CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref connCounts[index]); })));
            }

            var clients = new ConcurrentBag<ITDengineClient>();
            var errors = new ConcurrentQueue<Exception>();
            var startGate = new ManualResetEventSlim(false);
            try
            {
                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Start();
                }

                Thread.Sleep(200);

                var hostList = string.Join(",", ports.Select(p => $"127.0.0.1:{p}"));
                var connStr = "protocol=WebSocket;" +
                              $"host={hostList};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:05;";

                var tasks = new Task[clientCount];
                for (var i = 0; i < clientCount; i++)
                {
                    tasks[i] = Task.Run(() =>
                    {
                        startGate.Wait();
                        try
                        {
                            var client = DbDriver.Open(new ConnectionStringBuilder(connStr));
                            clients.Add(client);
                        }
                        catch (Exception ex)
                        {
                            errors.Enqueue(ex);
                        }
                    });
                }

                startGate.Set();
                Task.WaitAll(tasks);

                Assert.Empty(errors);
                Assert.Equal(clientCount, connCounts.Sum());
                var minCount = connCounts.Min();
                var maxCount = connCounts.Max();
                Assert.True(maxCount - minCount <= 2,
                    $"connection distribution is not balanced under moderate concurrency: [{string.Join(",", connCounts)}]");
            }
            finally
            {
                while (clients.TryTake(out var client))
                {
                    client.Dispose();
                }

                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Dispose();
                }

                startGate.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConnectShouldNotUseAddressFromOtherConnection()
        {
            var servedPort = GetFreePort();
            var unavailablePort = GetFreePort();
            while (unavailablePort == servedPort)
            {
                unavailablePort = GetFreePort();
            }
            var servedConnCount = 0;

            var servedServer = new MockWSServer(servedPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref servedConnCount); }));
            try
            {
                servedServer.Start();

                var servedConnStr = "protocol=WebSocket;" +
                                    $"host=127.0.0.1:{servedPort};" +
                                    "useSSL=false;" +
                                    "username=root;" +
                                    "password=taosdata;" +
                                    "enableCompression=true;" +
                                    "connTimeout=00:00:05;";
                using (var servedClient = DbDriver.Open(new ConnectionStringBuilder(servedConnStr)))
                {
                    var unavailableConnStr = "protocol=WebSocket;" +
                                             $"host=127.0.0.1:{unavailablePort};" +
                                             "useSSL=false;" +
                                             "username=root;" +
                                             "password=taosdata;" +
                                             "enableCompression=true;" +
                                             "connTimeout=00:00:02;";
                    Assert.ThrowsAny<Exception>(() =>
                    {
                        using (var shouldFailClient = DbDriver.Open(new ConnectionStringBuilder(unavailableConnStr)))
                        {
                        }
                    });
                }
            }
            finally
            {
                servedServer.Dispose();
            }

            Assert.Equal(1, Volatile.Read(ref servedConnCount));
        }

        [Fact]
        public void MultiAddressReconnectShouldPreferPreviousAddressForTransientDisconnect()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }
            var firstCacheKey = BuildWsCacheKey(firstPort);
            var secondCacheKey = BuildWsCacheKey(secondPort);
            ResetFailoverCacheConnectionCount(firstCacheKey);
            ResetFailoverCacheConnectionCount(secondCacheKey);

            var firstConnCount = 0;
            var secondConnCount = 0;
            var firstStmtInitClosed = 0;
            var connAttemptOrder = new ConcurrentQueue<string>();
            ulong stmtId = 0;

            Action<WebSocket, WebSocketMessageType, byte[]> firstHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        var resp = new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case WSAction.Conn:
                    {
                        connAttemptOrder.Enqueue("first");
                        Interlocked.Increment(ref firstConnCount);
                        var resp = new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case "stmt2_init":
                    {
                        if (Interlocked.CompareExchange(ref firstStmtInitClosed, 1, 0) == 0)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "",
                                CancellationToken.None).GetAwaiter().GetResult();
                            return;
                        }

                        var resp = new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                }
            };

            Action<WebSocket, WebSocketMessageType, byte[]> secondHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        var resp = new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case WSAction.Conn:
                    {
                        connAttemptOrder.Enqueue("second");
                        Interlocked.Increment(ref secondConnCount);
                        var resp = new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case "stmt2_init":
                    {
                        var resp = new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                }
            };

            var firstServer = new MockWSServer(firstPort, firstHandler);
            var secondServer = new MockWSServer(secondPort, secondHandler);
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort},127.0.0.1:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectRetryCount=5;" +
                              "reconnectIntervalMs=30;" +
                              "connTimeout=00:00:05;";
                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var stmt = client.StmtInit())
                    {
                        Assert.NotNull(stmt);
                    }

                    Assert.True(client.ConnectionAvailable());
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }

            var attempts = connAttemptOrder.ToArray();
            Assert.True(attempts.Length >= 2, $"expected at least two connection attempts, actual: [{string.Join(",", attempts)}]");
            Assert.Equal("first", attempts[0]);
            Assert.Equal("first", attempts[1]);
            Assert.Equal(2, Volatile.Read(ref firstConnCount));
            Assert.Equal(0, Volatile.Read(ref secondConnCount));
        }

        [Fact]
        public void ReconnectShouldReleaseOldLeaseAfterFailoverSuccess()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            var firstConnCount = 0;
            var secondConnCount = 0;
            var firstUnavailable = 0;
            ulong stmtId = 0;

            Action<WebSocket, WebSocketMessageType, byte[]> firstHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    case WSAction.Conn:
                    {
                        if (Volatile.Read(ref firstUnavailable) == 1)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "first address unavailable",
                                    CancellationToken.None)
                                .GetAwaiter().GetResult();
                            return;
                        }

                        if (req.Action == WSAction.Conn)
                        {
                            Interlocked.Increment(ref firstConnCount);
                            SendResponse(webSocket, messageType, new WSConnResp
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args == null ? 0 : req.Args.ReqId
                            });
                            break;
                        }

                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case "stmt2_init":
                    {
                        if (Interlocked.CompareExchange(ref firstUnavailable, 1, 0) == 0)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "force first reconnect fail",
                                    CancellationToken.None)
                                .GetAwaiter().GetResult();
                            return;
                        }

                        SendResponse(webSocket, messageType, new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        });
                        break;
                    }
                }
            };

            Action<WebSocket, WebSocketMessageType, byte[]> secondHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSAction.Conn:
                    {
                        Interlocked.Increment(ref secondConnCount);
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    case "stmt2_init":
                    {
                        SendResponse(webSocket, messageType, new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        });
                        break;
                    }
                }
            };

            var firstServer = new MockWSServer(firstPort, firstHandler);
            var secondServer = new MockWSServer(secondPort, secondHandler);
            var firstCacheKey = BuildWsCacheKey(firstPort);
            var secondCacheKey = BuildWsCacheKey(secondPort);
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort},127.0.0.1:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectRetryCount=5;" +
                              "reconnectIntervalMs=30;" +
                              "connTimeout=00:00:05;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.Equal(1, Volatile.Read(ref firstConnCount));
                    using (var stmt = client.StmtInit())
                    {
                        Assert.NotNull(stmt);
                    }

                    Assert.True(client.ConnectionAvailable());
                    Assert.Equal(0, GetFailoverCacheConnectionCount(firstCacheKey));
                    Assert.Equal(1, GetFailoverCacheConnectionCount(secondCacheKey));
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }

            Assert.Equal(0, GetFailoverCacheConnectionCount(firstCacheKey));
            Assert.Equal(0, GetFailoverCacheConnectionCount(secondCacheKey));
            Assert.Equal(1, Volatile.Read(ref firstConnCount));
            Assert.Equal(1, Volatile.Read(ref secondConnCount));
        }

        [Fact]
        public void DisposeAndReconnectRaceShouldReleaseBothLeases()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            var firstUnavailable = 0;
            var secondConnEntered = new ManualResetEventSlim(false);
            var releaseSecondConn = new ManualResetEventSlim(false);
            ulong stmtId = 0;

            Action<WebSocket, WebSocketMessageType, byte[]> firstHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    case WSAction.Conn:
                    {
                        if (Volatile.Read(ref firstUnavailable) == 1)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "first address unavailable",
                                    CancellationToken.None)
                                .GetAwaiter().GetResult();
                            return;
                        }

                        if (req.Action == WSAction.Conn)
                        {
                            SendResponse(webSocket, messageType, new WSConnResp
                            {
                                Code = 0,
                                Action = req.Action,
                                ReqId = req.Args == null ? 0 : req.Args.ReqId
                            });
                            break;
                        }

                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case "stmt2_init":
                    {
                        if (Interlocked.CompareExchange(ref firstUnavailable, 1, 0) == 0)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "force reconnect",
                                    CancellationToken.None)
                                .GetAwaiter().GetResult();
                            return;
                        }

                        SendResponse(webSocket, messageType, new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        });
                        break;
                    }
                }
            };

            Action<WebSocket, WebSocketMessageType, byte[]> secondHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSAction.Conn:
                    {
                        secondConnEntered.Set();
                        if (!releaseSecondConn.Wait(TimeSpan.FromSeconds(5)))
                        {
                            throw new TimeoutException("timed out waiting to release second reconnect connection");
                        }

                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    case "stmt2_init":
                    {
                        SendResponse(webSocket, messageType, new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        });
                        break;
                    }
                }
            };

            var firstServer = new MockWSServer(firstPort, firstHandler);
            var secondServer = new MockWSServer(secondPort, secondHandler);
            var firstCacheKey = BuildWsCacheKey(firstPort);
            var secondCacheKey = BuildWsCacheKey(secondPort);

            ITDengineClient client = null;
            Exception stmtException = null;
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort},127.0.0.1:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectRetryCount=5;" +
                              "reconnectIntervalMs=30;" +
                              "connTimeout=00:00:05;" +
                              "readTimeout=00:00:10;";

                client = DbDriver.Open(new ConnectionStringBuilder(connStr));
                var stmtTask = Task.Run(() =>
                {
                    try
                    {
                        using (var stmt = client.StmtInit())
                        {
                        }
                    }
                    catch (Exception ex)
                    {
                        stmtException = ex;
                    }
                });

                Assert.True(secondConnEntered.Wait(TimeSpan.FromSeconds(5)),
                    "reconnect should reach second address before dispose");

                var disposeTask = Task.Run(() => client.Dispose());
                Assert.True(disposeTask.Wait(TimeSpan.FromSeconds(2)),
                    "dispose should not block while reconnect is in flight");

                releaseSecondConn.Set();
                Assert.True(stmtTask.Wait(TimeSpan.FromSeconds(5)),
                    "stmt operation should complete after dispose");

                if (stmtException != null)
                {
                    Assert.True(stmtException is ObjectDisposedException || stmtException is TDengineError,
                        $"unexpected exception type during dispose/reconnect race: {stmtException.GetType().Name}");
                }
            }
            finally
            {
                releaseSecondConn.Set();
                try
                {
                    client?.Dispose();
                }
                catch
                {
                }

                firstServer.Dispose();
                secondServer.Dispose();
                secondConnEntered.Dispose();
                releaseSecondConn.Dispose();
            }

            Assert.Equal(0, GetFailoverCacheConnectionCount(firstCacheKey));
            Assert.Equal(0, GetFailoverCacheConnectionCount(secondCacheKey));
        }

        [Fact]
        public void MultiAddressDisposeShouldReleaseConnectionCountAndRebalance()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            var firstConnCount = 0;
            var secondConnCount = 0;

            var firstServer = new MockWSServer(firstPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref firstConnCount); }));
            var secondServer = new MockWSServer(secondPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref secondConnCount); }));
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort},127.0.0.1:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:05;";

                using (var firstClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var secondClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                    {
                        Assert.Equal(1, Volatile.Read(ref firstConnCount));
                        Assert.Equal(1, Volatile.Read(ref secondConnCount));
                        Assert.True(firstClient.ConnectionAvailable());
                        Assert.True(secondClient.ConnectionAvailable());
                    }
                }

                using (var thirdClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(thirdClient.ConnectionAvailable());
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }

            Assert.Equal(2, Volatile.Read(ref firstConnCount));
            Assert.Equal(1, Volatile.Read(ref secondConnCount));
        }

        [Fact]
        public void MethodsShouldThrowObjectDisposedExceptionAfterDispose()
        {
            var port = GetFreePort();
            var connected = 0;
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSAction.Conn:
                    {
                        Interlocked.Increment(ref connected);
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {req.Action}");
                }
            });

            ITDengineClient client = null;
            try
            {
                server.Start();
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{port};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectRetryCount=3;" +
                              "reconnectIntervalMs=10;";

                client = DbDriver.Open(new ConnectionStringBuilder(connStr));
                Assert.Equal(1, Volatile.Read(ref connected));
                client.Dispose();

                Assert.Throws<ObjectDisposedException>(() => client.Query("select server_version()"));
                Assert.Throws<ObjectDisposedException>(() => client.Exec("select server_version()"));
                Assert.Throws<ObjectDisposedException>(() => client.StmtInit());
                Assert.Throws<ObjectDisposedException>(() => client.SchemalessInsert(
                    new[] { "m1,t1=1 f1=1i64 1" },
                    TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                    TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS,
                    0,
                    ReqId.GetReqId()));
            }
            finally
            {
                try
                {
                    client?.Dispose();
                }
                catch
                {
                }

                server.Dispose();
            }
        }

        private static Action<WebSocket, WebSocketMessageType, byte[]> CreateHandshakeMessageHandler(Action onConnected)
        {
            return (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        var resp = new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case WSAction.Conn:
                    {
                        onConnected?.Invoke();
                        var resp = new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                }
            };
        }

        private static void SendResponse(WebSocket webSocket, WebSocketMessageType messageType, object response)
        {
            var respStr = JsonConvert.SerializeObject(response);
            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
            webSocket.SendAsync(data, messageType, true, CancellationToken.None).GetAwaiter().GetResult();
        }

        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var endpoint = (IPEndPoint)listener.LocalEndpoint;
            listener.Stop();
            return endpoint.Port;
        }

        private static string BuildWsCacheKey(int port)
        {
            return $"ws://127.0.0.1:{port}";
        }

        private static int GetFailoverCacheConnectionCount(string cacheKey)
        {
            var cacheType = typeof(FailoverAddress).Assembly.GetType("TDengine.Driver.FailoverAddressCache");
            Assert.NotNull(cacheType);

            var syncLockField = cacheType.GetField("SyncLock", BindingFlags.Static | BindingFlags.NonPublic);
            var countsField = cacheType.GetField("ConnectionCountByAddress",
                BindingFlags.Static | BindingFlags.NonPublic);
            Assert.NotNull(syncLockField);
            Assert.NotNull(countsField);

            var syncLock = syncLockField.GetValue(null);
            var counts = countsField.GetValue(null) as Dictionary<string, int>;
            Assert.NotNull(syncLock);
            Assert.NotNull(counts);

            lock (syncLock)
            {
                if (counts.TryGetValue(cacheKey, out var count))
                {
                    return count;
                }

                return 0;
            }
        }

        private static void ResetFailoverCacheConnectionCount(string cacheKey)
        {
            var cacheType = typeof(FailoverAddress).Assembly.GetType("TDengine.Driver.FailoverAddressCache");
            Assert.NotNull(cacheType);

            var syncLockField = cacheType.GetField("SyncLock", BindingFlags.Static | BindingFlags.NonPublic);
            var countsField = cacheType.GetField("ConnectionCountByAddress",
                BindingFlags.Static | BindingFlags.NonPublic);
            Assert.NotNull(syncLockField);
            Assert.NotNull(countsField);

            var syncLock = syncLockField.GetValue(null);
            var counts = countsField.GetValue(null) as Dictionary<string, int>;
            Assert.NotNull(syncLock);
            Assert.NotNull(counts);

            lock (syncLock)
            {
                counts.Remove(cacheKey);
            }
        }

        [Fact]
        public void SendAsyncShouldBeCancelledQuicklyWhenConnectionCloses()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            var firstCacheKey = BuildWsCacheKey(firstPort);
            var secondCacheKey = BuildWsCacheKey(secondPort);
            ResetFailoverCacheConnectionCount(firstCacheKey);
            ResetFailoverCacheConnectionCount(secondCacheKey);

            var firstConnCount = 0;
            var secondConnCount = 0;
            ulong stmtId = 0;
            var firstUnavailable = 0;

            Action<WebSocket, WebSocketMessageType, byte[]> firstHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null) throw new Exception("invalid websocket request");

                switch (req.Action)
                {
                    case WSAction.Version:
                    case WSAction.Conn:
                    {
                        if (Volatile.Read(ref firstUnavailable) == 1)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError,
                                    "first address unavailable", CancellationToken.None)
                                .GetAwaiter().GetResult();
                            return;
                        }

                        if (req.Action == WSAction.Conn)
                        {
                            Interlocked.Increment(ref firstConnCount);
                            SendResponse(webSocket, messageType, new WSConnResp
                            {
                                Code = 0, Action = req.Action,
                                ReqId = req.Args == null ? 0 : req.Args.ReqId
                            });
                            break;
                        }

                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case "stmt2_init":
                    {
                        // Mark first server as permanently unavailable, then close.
                        Interlocked.Exchange(ref firstUnavailable, 1);
                        Thread.Sleep(200);
                        webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "",
                            CancellationToken.None).GetAwaiter().GetResult();
                        break;
                    }
                }
            };

            Action<WebSocket, WebSocketMessageType, byte[]> secondHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null) throw new Exception("invalid websocket request");

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSAction.Conn:
                    {
                        Interlocked.Increment(ref secondConnCount);
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    case "stmt2_init":
                    {
                        SendResponse(webSocket, messageType, new WSStmt2InitResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        });
                        break;
                    }
                }
            };

            var firstServer = new MockWSServer(firstPort, firstHandler);
            var secondServer = new MockWSServer(secondPort, secondHandler);
            try
            {
                firstServer.Start();
                secondServer.Start();

                // Use a long writeTimeout (30s) to make the test meaningful —
                // before the fix, a blocked SendAsync would wait the full writeTimeout.
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort},127.0.0.1:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectRetryCount=5;" +
                              "reconnectIntervalMs=30;" +
                              "connTimeout=00:00:05;" +
                              "writeTimeout=00:00:30;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.Equal(1, Volatile.Read(ref firstConnCount));

                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    using (var stmt = client.StmtInit())
                    {
                        Assert.NotNull(stmt);
                    }

                    sw.Stop();

                    // The operation should complete well under the 30s writeTimeout.
                    // Allow up to 10s for CI environments, but the typical time is < 3s.
                    Assert.True(sw.Elapsed.TotalSeconds < 10,
                        $"failover took {sw.Elapsed.TotalSeconds:F1}s, expected < 10s (writeTimeout=30s)");

                    Assert.True(client.ConnectionAvailable());
                    Assert.True(Volatile.Read(ref secondConnCount) >= 1,
                        "should have reconnected to second server");
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }
        }

        private class TestBaseReq
        {
            [JsonProperty("req_id")] public ulong ReqId { get; set; }
        }
    }
}
