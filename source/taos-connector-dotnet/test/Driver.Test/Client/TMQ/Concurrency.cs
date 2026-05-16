using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Driver.Test.Client.Query;
using Newtonsoft.Json;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using TDengine.TMQ;
using Xunit;

namespace Driver.Test.Client.TMQ
{
    public partial class Consumer
    {
        [Fact]
        public void CommitBatchShouldNotBlockCloseAndShouldStillFinish()
        {
            var port = GetFreePort();
            var firstCommitStarted = new ManualResetEventSlim(false);
            var releaseFirstCommit = new ManualResetEventSlim(false);
            var commitCount = 0;
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var payload = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<MockRequestBase>>(payload);
                if (baseReq == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSTMQAction.TMQSubscribe:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQSubscribeReq>>(payload);
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    case WSTMQAction.TMQCommitOffset:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQCommitOffsetReq>>(payload);
                        if (req == null || req.Args == null)
                        {
                            throw new Exception("invalid commit offset request");
                        }

                        if (Interlocked.Increment(ref commitCount) == 1)
                        {
                            firstCommitStarted.Set();
                            if (!releaseFirstCommit.Wait(TimeSpan.FromSeconds(5)))
                            {
                                throw new TimeoutException("timed out waiting to release first commit");
                            }
                        }

                        SendResponse(webSocket, messageType, new WSTMQCommitOffsetResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req.Args.ReqId,
                            Topic = req.Args.Topic,
                            VGroupId = req.Args.VGroupId,
                            Offset = req.Args.Offset
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {baseReq.Action}");
                }
            });

            IConsumer<Dictionary<string, object>> consumer = null;
            try
            {
                server.Start();
                consumer = new ConsumerBuilder<Dictionary<string, object>>(BuildMockWsConfig(port)).Build();
                consumer.Subscribe("tmq_commit_close_race");

                var offsets = new[]
                {
                    new TopicPartitionOffset("tmq_commit_close_race", 0, 10),
                    new TopicPartitionOffset("tmq_commit_close_race", 0, 11),
                    new TopicPartitionOffset("tmq_commit_close_race", 0, 12)
                };

                var commitTask = Task.Run(() => consumer.Commit(offsets));
                Assert.True(firstCommitStarted.Wait(TimeSpan.FromSeconds(5)),
                    "first batch commit should reach the server");

                var closeTask = Task.Run(() => consumer.Close());
                Assert.True(closeTask.Wait(TimeSpan.FromSeconds(1)),
                    "close should not wait for the in-flight batch commit");

                releaseFirstCommit.Set();

                var completedCommit = Task.WhenAny(commitTask, Task.Delay(TimeSpan.FromSeconds(5)))
                    .GetAwaiter().GetResult();
                Assert.Same(commitTask, completedCommit);
                Assert.Equal(TaskStatus.RanToCompletion, commitTask.Status);

                var completedClose = Task.WhenAny(closeTask, Task.Delay(TimeSpan.FromSeconds(5)))
                    .GetAwaiter().GetResult();
                Assert.Same(closeTask, completedClose);
                Assert.Equal(TaskStatus.RanToCompletion, closeTask.Status);
                Assert.Equal(offsets.Length, Volatile.Read(ref commitCount));
            }
            finally
            {
                releaseFirstCommit.Set();
                try
                {
                    consumer?.Close();
                }
                catch
                {
                }

                server.Dispose();
                firstCommitStarted.Dispose();
                releaseFirstCommit.Dispose();
            }
        }

        [Fact]
        public void AutoCommitShouldNotBlockCloseOrDuplicateInFlightCommits()
        {
            var port = GetFreePort();
            var firstCommitStarted = new ManualResetEventSlim(false);
            var releaseFirstCommit = new ManualResetEventSlim(false);
            var commitCount = 0;
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var payload = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<MockRequestBase>>(payload);
                if (baseReq == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSTMQAction.TMQCommit:
                    {
                        if (Interlocked.Increment(ref commitCount) == 1)
                        {
                            firstCommitStarted.Set();
                            if (!releaseFirstCommit.Wait(TimeSpan.FromSeconds(5)))
                            {
                                throw new TimeoutException("timed out waiting to release first auto commit");
                            }
                        }

                        SendResponse(webSocket, messageType, new WSTMQCommitResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {baseReq.Action}");
                }
            });

            TDengine.TMQ.WebSocket.Consumer<Dictionary<string, object>> consumer = null;
            try
            {
                server.Start();
                consumer = Assert.IsType<TDengine.TMQ.WebSocket.Consumer<Dictionary<string, object>>>(
                    new ConsumerBuilder<Dictionary<string, object>>(BuildMockWsConfig(port, true)).Build());

                var autoCommitMethod = consumer.GetType()
                    .GetMethod("AutoCommitIfNeeded", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(autoCommitMethod);

                var autoCommitTask = Task.Run(() => autoCommitMethod.Invoke(consumer, null));
                Assert.True(firstCommitStarted.Wait(TimeSpan.FromSeconds(5)),
                    "auto commit should reach the server");

                var secondAutoCommitTask = Task.Run(() => autoCommitMethod.Invoke(consumer, null));
                Assert.True(secondAutoCommitTask.Wait(TimeSpan.FromSeconds(1)),
                    "a concurrent auto commit should not block behind the in-flight commit");
                Assert.Equal(1, Volatile.Read(ref commitCount));

                var closeTask = Task.Run(() => consumer.Close());
                Assert.True(closeTask.Wait(TimeSpan.FromSeconds(1)),
                    "close should not wait for the in-flight auto commit to finish");

                releaseFirstCommit.Set();

                var completedAutoCommit = Task.WhenAny(autoCommitTask, Task.Delay(TimeSpan.FromSeconds(5)))
                    .GetAwaiter().GetResult();
                Assert.Same(autoCommitTask, completedAutoCommit);
                Assert.True(autoCommitTask.IsCompleted, "the in-flight auto commit should complete after close");
                Assert.Equal(1, Volatile.Read(ref commitCount));
            }
            finally
            {
                releaseFirstCommit.Set();
                try
                {
                    consumer?.Close();
                }
                catch
                {
                }

                server.Dispose();
                firstCommitStarted.Dispose();
                releaseFirstCommit.Dispose();
            }
        }

        [Fact]
        public void ConsumeShouldIgnoreAutoCommitFailuresAndAvoidTightRetry()
        {
            var port = GetFreePort();
            var commitCount = 0;
            var pollCount = 0;
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var payload = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<MockRequestBase>>(payload);
                if (baseReq == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSTMQAction.TMQCommit:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQCommitReq>>(payload);
                        Interlocked.Increment(ref commitCount);
                        SendResponse(webSocket, messageType, new WSTMQCommitResp
                        {
                            Code = 1,
                            Message = "mock commit failure",
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    case WSTMQAction.TMQPoll:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQPollReq>>(payload);
                        Interlocked.Increment(ref pollCount);
                        SendResponse(webSocket, messageType, new WSTMQPollResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId,
                            HaveMessage = false
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {baseReq.Action}");
                }
            });

            IConsumer<Dictionary<string, object>> consumer = null;
            try
            {
                server.Start();
                var config = BuildMockWsConfig(port, enableAutoCommit: true);
                config["auto.commit.interval.ms"] = "1000";
                consumer = new ConsumerBuilder<Dictionary<string, object>>(config).Build();

                var first = consumer.Consume(0);
                var second = consumer.Consume(0);

                Assert.Null(first);
                Assert.Null(second);
                Assert.Equal(1, Volatile.Read(ref commitCount));
                Assert.Equal(2, Volatile.Read(ref pollCount));
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

                server.Dispose();
            }
        }

        [Fact]
        public void AutoCommitShouldScheduleFromCompletionTime()
        {
            var port = GetFreePort();
            var commitCount = 0;
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var payload = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<MockRequestBase>>(payload);
                if (baseReq == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSTMQAction.TMQCommit:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQCommitReq>>(payload);
                        if (Interlocked.Increment(ref commitCount) == 1)
                        {
                            Thread.Sleep(250);
                        }

                        SendResponse(webSocket, messageType, new WSTMQCommitResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {baseReq.Action}");
                }
            });

            TDengine.TMQ.WebSocket.Consumer<Dictionary<string, object>> consumer = null;
            try
            {
                server.Start();
                var config = BuildMockWsConfig(port, enableAutoCommit: true);
                config["auto.commit.interval.ms"] = "100";
                consumer = Assert.IsType<TDengine.TMQ.WebSocket.Consumer<Dictionary<string, object>>>(
                    new ConsumerBuilder<Dictionary<string, object>>(config).Build());

                var autoCommitMethod = consumer.GetType()
                    .GetMethod("AutoCommitIfNeeded", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(autoCommitMethod);

                autoCommitMethod.Invoke(consumer, null);
                autoCommitMethod.Invoke(consumer, null);
                Assert.Equal(1, Volatile.Read(ref commitCount));

                Thread.Sleep(150);
                autoCommitMethod.Invoke(consumer, null);
                Assert.Equal(2, Volatile.Read(ref commitCount));
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

                server.Dispose();
            }
        }

        [Fact]
        public void ConsumeShouldReconnectAfterAutoCommitDisconnect()
        {
            var port = GetFreePort();
            var versionCount = 0;
            var pollCount = 0;
            var disconnectedOnCommit = 0;
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var payload = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<MockRequestBase>>(payload);
                if (baseReq == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                    {
                        Interlocked.Increment(ref versionCount);
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSTMQAction.TMQCommit:
                    {
                        if (Interlocked.CompareExchange(ref disconnectedOnCommit, 1, 0) == 0)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "force commit disconnect",
                                    CancellationToken.None)
                                .GetAwaiter().GetResult();
                            break;
                        }

                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQCommitReq>>(payload);
                        SendResponse(webSocket, messageType, new WSTMQCommitResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    case WSTMQAction.TMQPoll:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQPollReq>>(payload);
                        Interlocked.Increment(ref pollCount);
                        SendResponse(webSocket, messageType, new WSTMQPollResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId,
                            HaveMessage = false
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {baseReq.Action}");
                }
            });

            IConsumer<Dictionary<string, object>> consumer = null;
            try
            {
                server.Start();
                var config = BuildMockWsConfig(port, enableAutoCommit: true, enableReconnect: true);
                config["auto.commit.interval.ms"] = "0";
                consumer = new ConsumerBuilder<Dictionary<string, object>>(config).Build();

                var result = consumer.Consume(0);
                Assert.Null(result);
                Assert.Equal(1, Volatile.Read(ref disconnectedOnCommit));
                Assert.Equal(1, Volatile.Read(ref pollCount));
                Assert.True(Volatile.Read(ref versionCount) >= 2,
                    "consume should reconnect after commit-side disconnect");
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

                server.Dispose();
            }
        }

        [Fact]
        public void SubscribeReconnectShouldPersistTopicsForFutureReconnects()
        {
            var port = GetFreePort();
            var subscribeCount = 0;
            var pollCount = 0;
            var topic = "tmq_reconnect_topics";
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var payload = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<MockRequestBase>>(payload);
                if (baseReq == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    case WSTMQAction.TMQSubscribe:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQSubscribeReq>>(payload);
                        var currentSubscribe = Interlocked.Increment(ref subscribeCount);
                        if (currentSubscribe == 1)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "force subscribe reconnect",
                                    CancellationToken.None)
                                .GetAwaiter().GetResult();
                            break;
                        }

                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    }
                    case WSTMQAction.TMQPoll:
                    {
                        var req = JsonConvert.DeserializeObject<WSActionReq<WSTMQPollReq>>(payload);
                        var currentPoll = Interlocked.Increment(ref pollCount);
                        if (currentPoll == 1)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "force consume reconnect",
                                    CancellationToken.None)
                                .GetAwaiter().GetResult();
                            break;
                        }

                        SendResponse(webSocket, messageType, new WSTMQPollResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = req?.Args == null ? 0 : req.Args.ReqId,
                            HaveMessage = false
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {baseReq.Action}");
                }
            });

            TDengine.TMQ.WebSocket.Consumer<Dictionary<string, object>> consumer = null;
            try
            {
                server.Start();
                consumer = Assert.IsType<TDengine.TMQ.WebSocket.Consumer<Dictionary<string, object>>>(
                    new ConsumerBuilder<Dictionary<string, object>>(BuildMockWsConfig(port, enableReconnect: true))
                        .Build());

                consumer.Subscribe(topic);

                var topicsField = consumer.GetType().GetField("_topics", BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.NotNull(topicsField);
                var savedTopics = Assert.IsType<List<string>>(topicsField.GetValue(consumer));
                Assert.Single(savedTopics);
                Assert.Equal(topic, savedTopics[0]);

                var result = consumer.Consume(0);
                Assert.Null(result);
                Assert.Equal(3, Volatile.Read(ref subscribeCount));
                Assert.Equal(2, Volatile.Read(ref pollCount));
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

                server.Dispose();
            }
        }

        [Fact]
        public void MethodsShouldThrowObjectDisposedExceptionAfterClose()
        {
            var port = GetFreePort();
            var server = new MockWSServer(port, (webSocket, messageType, message) =>
            {
                var payload = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<MockRequestBase>>(payload);
                if (baseReq == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                    {
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0,
                            Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    }
                    default:
                        throw new Exception($"unexpected websocket action: {baseReq.Action}");
                }
            });

            IConsumer<Dictionary<string, object>> consumer = null;
            try
            {
                server.Start();
                var config = BuildMockWsConfig(port);
                consumer = new ConsumerBuilder<Dictionary<string, object>>(config).Build();
                consumer.Close();

                Assert.Throws<ObjectDisposedException>(() => consumer.Consume(0));
                Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
                Assert.Throws<ObjectDisposedException>(() => consumer.Subscribe("topic_after_close"));
                Assert.Throws<ObjectDisposedException>(() => consumer.Commit());
                Assert.Throws<ObjectDisposedException>(() =>
                    consumer.Seek(new TopicPartitionOffset("topic_after_close", 0, 0)));
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

                server.Dispose();
            }
        }

        private static Dictionary<string, string> BuildMockWsConfig(int port, bool enableAutoCommit = false,
            bool enableReconnect = false)
        {
            var config = new Dictionary<string, string>
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", $"tmq_mock_group_{Guid.NewGuid():N}" },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", $"localhost:{port}" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "client.id", $"tmq_mock_client_{Guid.NewGuid():N}" },
                { "enable.auto.commit", enableAutoCommit ? "true" : "false" },
                { "useSSL", "false" }
            };

            if (enableAutoCommit)
            {
                config["auto.commit.interval.ms"] = "0";
            }

            if (enableReconnect)
            {
                config["ws.autoReconnect"] = "true";
                config["ws.reconnect.retry.count"] = "3";
                config["ws.reconnect.interval.ms"] = "1";
            }

            return config;
        }

        private static void SendResponse(WebSocket webSocket, WebSocketMessageType messageType, object response)
        {
            var respStr = JsonConvert.SerializeObject(response);
            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
            webSocket.SendAsync(data, messageType, true, CancellationToken.None).GetAwaiter().GetResult();
        }

        private class MockRequestBase
        {
            [JsonProperty("req_id")] public ulong ReqId { get; set; }
        }
    }
}
