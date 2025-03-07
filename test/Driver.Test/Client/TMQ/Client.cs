using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.TMQ;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Client.TMQ
{
    public partial class Consumer
    {
        private readonly ITestOutputHelper _output;
        private readonly string _nativeConnectString;
        private readonly string _wsConnectString;
        private readonly string _createTableSql;
        private readonly Dictionary<string, string> _nativeTMQCfg;
        private readonly Dictionary<string, string> _nativeTMQCfgAutoCommit;
        private readonly Dictionary<string, string> _wsTMQCfg;
        private readonly Dictionary<string, string> _wsTMQCfgAutoCommit;


        public Consumer(ITestOutputHelper output)
        {
            this._output = output;
            this._nativeConnectString = "host=127.0.0.1;port=6030;username=root;password=taosdata";
            this._wsConnectString =
                "protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true";

            this._createTableSql = "create table if not exists all_type(ts timestamp," +
                                   "c1 bool," +
                                   "c2 tinyint," +
                                   "c3 smallint," +
                                   "c4 int," +
                                   "c5 bigint," +
                                   "c6 tinyint unsigned," +
                                   "c7 smallint unsigned," +
                                   "c8 int unsigned," +
                                   "c9 bigint unsigned," +
                                   "c10 float," +
                                   "c11 double," +
                                   "c12 binary(20)," +
                                   "c13 nchar(20)," +
                                   "c14 varbinary(20)," +
                                   "c15 geometry(100)" +
                                   ")" +
                                   "tags(t1 int)";

            this._nativeTMQCfg = new Dictionary<string, string>()
            {
                { "group.id", "test" },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", "127.0.0.1" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "td.connect.port", "6030" },
                { "client.id", "test_tmq_c" },
                { "enable.auto.commit", "false" },
                { "msg.with.table.name", "true" },
                { "session.timeout.ms", "12000" },
                { "max.poll.interval.ms", "300000" },
                { "min.poll.rows", "20" },
            };

            this._nativeTMQCfgAutoCommit = new Dictionary<string, string>()
            {
                { "group.id", "test" },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", "127.0.0.1" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "td.connect.port", "6030" },
                { "client.id", "test_tmq_c" },
                { "enable.auto.commit", "true" },
                { "auto.commit.interval.ms", "100" },
                { "msg.with.table.name", "true" },
                { "session.timeout.ms", "12000" },
                { "max.poll.interval.ms", "300000" }
            };

            this._wsTMQCfg = new Dictionary<string, string>()
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", "test" },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", "127.0.0.1" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "td.connect.port", "6041" },
                { "client.id", "test_tmq_c" },
                { "enable.auto.commit", "false" },
                { "msg.with.table.name", "true" },
                { "useSSL", "false" },
                { "ws.message.enableCompression", "true" },
                { "session.timeout.ms", "12000" },
                { "max.poll.interval.ms", "300000" },
                { "min.poll.rows", "20" },
            };

            this._wsTMQCfgAutoCommit = new Dictionary<string, string>()
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", "test" },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", "localhost" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "td.connect.port", "6041" },
                { "client.id", "test_tmq_c" },
                { "enable.auto.commit", "true" },
                { "auto.commit.interval.ms", "100" },
                { "msg.with.table.name", "true" },
                { "useSSL", "false" },
                { "ws.message.enableCompression", "true" },
                { "session.timeout.ms", "12000" },
                { "max.poll.interval.ms", "300000" }
            };
        }

        private void NewConsumerTest(string connectString, string db, string topic, Dictionary<string, string> cfg)
        {
            var builder =
                new ConnectionStringBuilder(connectString);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    string[] sqlCommands =
                    {
                        $"drop topic if exists {topic}",
                        $"drop database if exists {db}",
                        $"create database if not exists {db}  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        $"use {db}",
                        this._createTableSql,
                        "create table if not exists ct0 using all_type tags(1000)",
                        "create table if not exists ct1 using all_type tags(2000)",
                        "create table if not exists ct2 using all_type tags(3000)",
                        $"create topic if not exists {topic} as stable all_type"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);
                    for (int i = 0; i < 3; i++)
                    {
                        var sql =
                            $"insert into ct{i} values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)')";
                        DoRequest(client, sql);
                    }

                    var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                    consumer.Subscribe($"{topic}");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal($"{topic}", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var position1 = consumer.Position(assignment[0]);
                    Assert.Equal(0, position1);
                    var position2 = consumer.Position(assignment[1]);
                    Assert.Equal(0, position2);
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Value.ToString());
                            }

                            consumer.Commit(new List<TopicPartitionOffset>
                            {
                                result.TopicPartitionOffset,
                            });
                            var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                                TimeSpan.Zero);
                            Assert.Single(committed);
                            Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                        }
                    }

                    Assert.Equal(3, messageCount);
                    consumer.Unsubscribe();
                    consumer.Close();
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop topic if exists {topic}");
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop database if exists {db}");
                }
            }
        }

        private void DoRequest(ITDengineClient client, string sql)
        {
            client.Exec(sql);
        }

        private void ConsumerSeekTest(string connectString, string db, string topic, Dictionary<string, string> cfg)
        {
            var builder =
                new ConnectionStringBuilder(connectString);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    string[] sqlCommands =
                    {
                        $"drop topic if exists {topic}",
                        $"drop database if exists {db}",
                        $"create database if not exists {db}  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        $"use {db}",
                        this._createTableSql,
                        "create table if not exists ct0 using all_type tags(1000)",
                        "create table if not exists ct1 using all_type tags(2000)",
                        "create table if not exists ct2 using all_type tags(3000)",
                        $"create topic if not exists {topic} as stable all_type"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);
                    for (int i = 0; i < 3; i++)
                    {
                        var sql =
                            $"insert into ct{i} values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)')";
                        DoRequest(client, sql);
                    }

                    var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                    consumer.Subscribe($"{topic}");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal($"{topic}", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var position1 = consumer.Position(assignment[0]);
                    Assert.Equal(0, position1);
                    var position2 = consumer.Position(assignment[1]);
                    Assert.Equal(0, position2);
                    // poll
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Value.ToString());
                            }

                            consumer.Commit(new List<TopicPartitionOffset>
                            {
                                result.TopicPartitionOffset,
                            });
                            var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                                TimeSpan.Zero);
                            Assert.Single(committed);
                            Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                        }
                    }

                    Assert.Equal(3, messageCount);
                    // seek
                    foreach (var topicPartition in assignment)
                    {
                        consumer.Seek(new TopicPartitionOffset(topicPartition.Topic, topicPartition.Partition, 0));
                    }

                    // poll after seek
                    messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Value.ToString());
                            }

                            consumer.Commit(new List<TopicPartitionOffset>
                            {
                                result.TopicPartitionOffset,
                            });
                            var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                                TimeSpan.Zero);
                            Assert.Single(committed);
                            Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                        }
                    }

                    Assert.Equal(3, messageCount);


                    consumer.Unsubscribe();
                    consumer.Close();
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop topic if exists {topic}");
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop database if exists {db}");
                }
            }
        }

        private void ConsumerCommitTest(string connectString, string db, string topic, Dictionary<string, string> cfg)
        {
            var builder = new ConnectionStringBuilder(connectString);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    string[] sqlCommands =
                    {
                        $"drop topic if exists {topic}",
                        $"drop database if exists {db}",
                        $"create database if not exists {db}  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        $"use {db}",
                        this._createTableSql,
                        "create table if not exists ct0 using all_type tags(1000)",
                        "create table if not exists ct1 using all_type tags(2000)",
                        "create table if not exists ct2 using all_type tags(3000)",
                        $"create topic if not exists {topic} as stable all_type"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);
                    for (int i = 0; i < 3; i++)
                    {
                        var sql =
                            $"insert into ct{i} values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)')";
                        DoRequest(client, sql);
                    }

                    var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                    consumer.Subscribe($"{topic}");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal($"{topic}", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var position1 = consumer.Position(assignment[0]);
                    Assert.Equal(0, position1);
                    var position2 = consumer.Position(assignment[1]);
                    Assert.Equal(0, position2);
                    // poll
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Value.ToString());
                            }

                            var committed = consumer.Commit();

                            Assert.Equal(2, committed.Count);
                            foreach (var c in committed)
                            {
                                if (c.Partition == result.Partition)
                                {
                                    Assert.NotEqual(0, c.Offset);
                                }
                            }

                            var allCommitted = consumer.Committed(TimeSpan.Zero);
                            allCommitted.Sort((x, y) => x.Partition.Value.CompareTo(y.Partition.Value));
                            committed.Sort((x, y) => x.Partition.Value.CompareTo(y.Partition.Value));
                            Assert.Equal(committed, allCommitted);
                        }
                    }

                    Assert.Equal(3, messageCount);
                    consumer.Unsubscribe();
                    consumer.Close();
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop topic if exists {topic}");
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop database if exists {db}");
                }
            }
        }

        private void ConsumerAutoCommitTest(string connectString, string db, string topic,
            Dictionary<string, string> cfg)
        {
            var builder = new ConnectionStringBuilder(connectString);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    string[] sqlCommands =
                    {
                        $"drop topic if exists {topic}",
                        $"drop database if exists {db}",
                        $"create database if not exists {db}  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        $"use {db}",
                        this._createTableSql,
                        "create table if not exists ct0 using all_type tags(1000)",
                        "create table if not exists ct1 using all_type tags(2000)",
                        "create table if not exists ct2 using all_type tags(3000)",
                        $"create topic if not exists {topic} as stable all_type"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);
                    for (int i = 0; i < 3; i++)
                    {
                        var sql =
                            $"insert into ct{i} values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)')";
                        DoRequest(client, sql);
                    }

                    var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                    consumer.Subscribe($"{topic}");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal($"{topic}", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var position1 = consumer.Position(assignment[0]);
                    Assert.Equal(0, position1);
                    var position2 = consumer.Position(assignment[1]);
                    Assert.Equal(0, position2);
                    // poll
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(100))
                        {
                            if (messageCount == 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                _output.WriteLine(message.Value.ToString());
                            }
                        }
                    }

                    Thread.Sleep(3000);
                    using (var result = consumer.Consume(100))
                    {
                    }

                    Assert.Equal(3, messageCount);
                    var committed = consumer.Committed(TimeSpan.Zero);
                    Assert.Equal(2, committed.Count);
                    foreach (var c in committed)
                    {
                        Assert.True(c.Offset > 0);
                    }

                    consumer.Unsubscribe();
                    consumer.Close();
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop topic if exists {topic}");
                    Thread.Sleep(3000);
                    DoRequest(client, $"drop database if exists {db}");
                }
            }
        }
    }
}