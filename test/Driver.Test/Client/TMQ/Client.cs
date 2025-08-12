using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
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
        private readonly string _cloudConnectString;
        private readonly Dictionary<string, string> _nativeTMQCfg;
        private readonly Dictionary<string, string> _nativeTMQCfgAutoCommit;
        private readonly Dictionary<string, string> _wsTMQCfg;
        private readonly Dictionary<string, string> _wsTMQCfgAutoCommit;
        private readonly Dictionary<string, string> _cloudTMQCfg;


        public Consumer(ITestOutputHelper output)
        {
            this._output = output;
            this._nativeConnectString = "host=localhost;port=6030;username=root;password=taosdata";
            this._wsConnectString =
                "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true";
            var cloudHost = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_ENDPOINT");
            var cloudToken = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_TOKEN");
            if (!string.IsNullOrEmpty(cloudHost) && !string.IsNullOrEmpty(cloudToken))
            {
                this._cloudConnectString = GetCloudConnectString(cloudHost, cloudToken);
                var now = DateTime.Now;
                var clientId = $"cs_test_{now.Ticks}";
                var goupId = $"cs_test_group_{now.Ticks}";
                this._cloudTMQCfg = new Dictionary<string, string>()
                {
                    { "td.connect.type", "WebSocket" },
                    { "group.id", goupId },
                    { "auto.offset.reset", "latest" },
                    { "td.connect.ip", cloudHost },
                    { "token", cloudToken },
                    { "td.connect.port", "443" },
                    { "client.id", clientId },
                    { "enable.auto.commit", "false" },
                    { "msg.with.table.name", "true" },
                    { "useSSL", "true" },
                    { "ws.message.enableCompression", "true" },
                    { "session.timeout.ms", "12000" },
                    { "max.poll.interval.ms", "300000" },
                    { "min.poll.rows", "20" },
                };
            }

            this._createTableSql = "create table if not exists tmq_all_type_decimal(ts timestamp," +
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
                                   "c15 geometry(100)," +
                                   "c16 decimal(20,4)," +
                                   "c17 decimal(8,4)" +
                                   ")" +
                                   "tags(t1 int)";

            this._nativeTMQCfg = new Dictionary<string, string>()
            {
                { "group.id", "test" },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", "localhost" },
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
                { "td.connect.ip", "localhost" },
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
                { "td.connect.ip", "localhost" },
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

        private static string GetCloudConnectString(string host, string token)
        {
            return
                $"protocol=WebSocket;host={host};port=443;useSSL=true;token={token};enableCompression=true";
        }

        private static bool IsCloudTest(Dictionary<string, string> cfg)
        {
            return cfg.ContainsKey("token") && !string.IsNullOrEmpty(cfg["token"]);
        }

        private void checkValue(Dictionary<string, object> value)
        {
            Assert.Equal(true, value["c1"]);
            Assert.Equal((sbyte)2, value["c2"]);
            Assert.Equal((short)3, value["c3"]);
            Assert.Equal(4, value["c4"]);
            Assert.Equal((long)5, value["c5"]);
            Assert.Equal((byte)6, value["c6"]);
            Assert.Equal((ushort)7, value["c7"]);
            Assert.Equal((uint)8, value["c8"]);
            Assert.Equal((ulong)9, value["c9"]);
            Assert.Equal((float)10, value["c10"]);
            Assert.Equal((double)11, value["c11"]);
            Assert.Equal(Encoding.UTF8.GetBytes("binary"), value["c12"]);
            Assert.Equal("nchar", value["c13"]);
            Assert.Equal(Encoding.UTF8.GetBytes("varbinary"), value["c14"]);
            Assert.Equal(new byte[]
            {
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x59, 0x40
            }, value["c15"]);
            Assert.Equal(decimal.Parse("6581493296132535.4860"), value["c16"]);
            Assert.Equal(decimal.Parse("6581.4932"), value["c17"]);
        }

        private void NewConsumerTest(string connectString, string db, string topic, Dictionary<string, string> cfg)
        {
            var builder =
                new ConnectionStringBuilder(connectString);
            using (var client = DbDriver.Open(builder))
            {
                var isCloud = IsCloudTest(cfg);
                try
                {
                    string[] sqlCommands =
                    {
                        $"drop topic if exists {topic}",
                        $"drop database if exists {db}",
                        $"create database if not exists {db}  vgroups 2  WAL_RETENTION_PERIOD 86400",
                        $"use {db}",
                        this._createTableSql,
                        "create table if not exists ct0_decimal using tmq_all_type_decimal tags(1000)",
                        "create table if not exists ct1_decimal using tmq_all_type_decimal tags(2000)",
                        "create table if not exists ct2_decimal using tmq_all_type_decimal tags(3000)",
                        $"create topic if not exists {topic} as stable tmq_all_type_decimal"
                    };
                    if (isCloud)
                    {
                        sqlCommands = new string[]
                        {
                            $"use {db}",
                            "create table if not exists ct0_decimal using tmq_all_type_decimal tags(1000)",
                            "create table if not exists ct1_decimal using tmq_all_type_decimal tags(2000)",
                            "create table if not exists ct2_decimal using tmq_all_type_decimal tags(3000)",
                        };
                    }

                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    DateTime now = new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour,
                        dateTime.Minute,
                        dateTime.Second, dateTime.Millisecond, dateTime.Kind);

                    var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                    consumer.Subscribe($"{topic}");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal($"{topic}", topics[0]);
                    _output.WriteLine(assignment.ToString());
                    var messageCount = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(500))
                        {
                            _output.WriteLine($"{result}");
                            // cloud may insert data by other process
                            if (messageCount >= 3)
                            {
                                break;
                            }

                            if (result == null)
                            {
                                for (int j = 0; j < 3; j++)
                                {
                                    var sql =
                                        $"insert into ct{j}_decimal values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)',6581493296132535.4860,6581.4932)";
                                    DoRequest(client, sql);
                                }

                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                _output.WriteLine(message.TableName);
                                checkValue(message.Value);
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

                    // cloud may insert data by other process
                    Assert.True(messageCount >= 3);
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
                    if (!isCloud)
                    {
                        Thread.Sleep(3000);
                        DoRequest(client, $"drop topic if exists {topic}");
                        Thread.Sleep(3000);
                        DoRequest(client, $"drop database if exists {db}");
                    }
                    else
                    {
                        var groupId = cfg["group.id"];
                        for (int i = 0; i < 20; i++)
                        {
                            Thread.Sleep(1000);
                            try
                            {
                                DoRequest(client, $"DROP CONSUMER GROUP IF EXISTS {groupId} on {topic}");
                                break;
                            }
                            catch (TDengineError e)
                            {
                                _output.WriteLine(e.ToString());
                            }
                        }
                    }
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
                        "create table if not exists ct0_decimal using tmq_all_type_decimal tags(1000)",
                        "create table if not exists ct1_decimal using tmq_all_type_decimal tags(2000)",
                        "create table if not exists ct2_decimal using tmq_all_type_decimal tags(3000)",
                        $"create topic if not exists {topic} as stable tmq_all_type_decimal"
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
                            $"insert into ct{i}_decimal values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)',6581493296132535.4860,6581.4932)";
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
                                checkValue(message.Value);
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
                                checkValue(message.Value);
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
                        "create table if not exists ct0_decimal using tmq_all_type_decimal tags(1000)",
                        "create table if not exists ct1_decimal using tmq_all_type_decimal tags(2000)",
                        "create table if not exists ct2_decimal using tmq_all_type_decimal tags(3000)",
                        $"create topic if not exists {topic} as stable tmq_all_type_decimal"
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
                            $"insert into ct{i}_decimal values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)',6581493296132535.4860,6581.4932)";
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
                                checkValue(message.Value);
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
                        "create table if not exists ct0_decimal using tmq_all_type_decimal tags(1000)",
                        "create table if not exists ct1_decimal using tmq_all_type_decimal tags(2000)",
                        "create table if not exists ct2_decimal using tmq_all_type_decimal tags(3000)",
                        $"create topic if not exists {topic} as stable tmq_all_type_decimal"
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
                            $"insert into ct{i}_decimal values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)',6581493296132535.4860,6581.4932)";
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
                                checkValue(message.Value);
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

        // test consumer multi poll without duplicating messages
        private void ConsumerMultiPollTest(string connectString, string db, string topic,
            Dictionary<string, string> cfg)
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
                        "create table t(ts timestamp,v int)",
                        $"create topic if not exists {topic} as select * from t"
                    };
                    foreach (var sqlCommand in sqlCommands)
                    {
                        DoRequest(client, sqlCommand);
                    }

                    DateTime dateTime = DateTime.Now;
                    var nowTs = TDengineConstant.ConvertDateTimeToTimestamp(dateTime,
                        TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                    var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                    consumer.Subscribe($"{topic}");
                    var assignment = consumer.Assignment;
                    Assert.Equal(2, assignment.Count);
                    var topics = consumer.Subscription();
                    Assert.Single(topics);
                    Assert.Equal($"{topic}", topics[0]);
                    var messageCount = 0;
                    var insertIndex = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        using (var result = consumer.Consume(500))
                        {
                            if (result == null)
                            {
                                if (i == 0)
                                {
                                    // insert data for the first time
                                    var sql = $"insert into t values('{nowTs}',{0})";
                                    DoRequest(client, sql);
                                }

                                continue;
                            }

                            foreach (var message in result.Message)
                            {
                                messageCount += 1;
                                // check message
                                var tsData = (DateTime)message.Value["ts"];
                                var v = (int)message.Value["v"];
                                var ts = TDengineConstant.ConvertDateTimeToTimestamp(tsData,
                                    TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                Assert.Equal(insertIndex, v);
                                Assert.Equal(nowTs + 1000 * insertIndex, ts);
                            }

                            consumer.Commit(new List<TopicPartitionOffset>
                            {
                                result.TopicPartitionOffset,
                            });
                            var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                                TimeSpan.Zero);
                            Assert.Single(committed);
                            Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                            // insert next data
                            insertIndex++;
                            DoRequest(client, $"insert into t values('{nowTs + 1000 * insertIndex}',{insertIndex})");
                        }
                    }

                    Assert.True(messageCount > 1);
                    // check message count
                    Assert.Equal(4, messageCount);
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


        private void ConsumerTimezoneTest(string connectString, string db, string topic, string timezone,
            Dictionary<string, string> cfg)
        {
            if (Environment.Version.Major < 6)
            {
                _output.WriteLine($"Dotnet Version is {Environment.Version}. Skipping ConsumerTimezoneTest.");
                return;
            }

            var builder =
                new ConnectionStringBuilder(connectString);
            var tz = TimeZoneInfo.FindSystemTimeZoneById(timezone);
            builder.ConnectionTimezone = tz;
            ITDengineClient client = null;
            try
            {
                client = DbDriver.Open(builder);
            }
            catch (TDengineError e)
            {
                if (e.Code != 0x237) throw;
                _output.WriteLine(
                    $"TDengineError: {e.Code} - {e.Message}. Skipping QueryWithConnectionTimezoneTest.");
                return;
            }

            try
            {
                string[] sqlCommands =
                {
                    $"drop topic if exists {topic}",
                    $"drop database if exists {db}",
                    $"create database if not exists {db}  vgroups 2  WAL_RETENTION_PERIOD 86400",
                    $"use {db}",
                    "create table t(ts timestamp,v int)",
                    $"create topic if not exists {topic} as select * from t"
                };
                foreach (var sqlCommand in sqlCommands)
                {
                    DoRequest(client, sqlCommand);
                }

                DateTime dateTime = DateTime.Now;
                var nowTs = TDengineConstant.ConvertDateTimeToTimestamp(dateTime,
                    TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                var insertTime =
                    TDengineConstant.ConvertTimestampToDateTime(nowTs, TDenginePrecision.TSDB_TIME_PRECISION_MILLI,
                        tz);
                var insertTimeStr = insertTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                Dictionary<string, string> copyCfg = new Dictionary<string, string>(cfg)
                {
                    ["connectionTimezone"] = timezone
                };
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(copyCfg).Build();
                consumer.Subscribe($"{topic}");
                var assignment = consumer.Assignment;
                Assert.Equal(2, assignment.Count);
                var topics = consumer.Subscription();
                Assert.Single(topics);
                Assert.Equal($"{topic}", topics[0]);
                var messageCount = 0;
                var insertIndex = 0;
                for (int i = 0; i < 5; i++)
                {
                    using (var result = consumer.Consume(500))
                    {
                        if (result == null)
                        {
                            if (i == 0)
                            {
                                // insert data for the first time
                                var sql = $"insert into t values('{insertTimeStr}',{0})";
                                _output.WriteLine(sql);
                                DoRequest(client, sql);
                            }

                            continue;
                        }

                        foreach (var message in result.Message)
                        {
                            messageCount += 1;
                            // check message
                            var tsData = (DateTime)message.Value["ts"];
                            var v = (int)message.Value["v"];
                            var ts = TDengineConstant.ConvertDateTimeToTimestamp(tsData,
                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI, tz);
                            var tsDataStr = tsData.ToString("yyyy-MM-dd HH:mm:ss.fff");
                            var expectStr = TDengineConstant
                                .ConvertTimestampToDateTime(nowTs + 1000 * insertIndex,
                                    TDenginePrecision.TSDB_TIME_PRECISION_MILLI, tz)
                                .ToString("yyyy-MM-dd HH:mm:ss.fff");
                            Assert.Equal(insertIndex, v);
                            Assert.Equal(nowTs + 1000 * insertIndex, ts);
                            Assert.Equal(expectStr, tsDataStr);
                        }

                        consumer.Commit(new List<TopicPartitionOffset>
                        {
                            result.TopicPartitionOffset,
                        });
                        var committed = consumer.Committed(new TopicPartition[] { result.TopicPartition },
                            TimeSpan.Zero);
                        Assert.Single(committed);
                        Assert.Equal(result.TopicPartitionOffset.Offset, committed[0].Offset);
                        // insert next data
                        insertIndex++;
                        DoRequest(client, $"insert into t values('{nowTs + 1000 * insertIndex}',{insertIndex})");
                    }
                }

                Assert.True(messageCount > 1);
                // check message count
                Assert.Equal(4, messageCount);
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
                client?.Dispose();
            }
        }

        private class TestDeserializer : IDeserializer<bool>
        {
            private readonly long _timestamp;
            private readonly TDenginePrecision _precision;

            public TestDeserializer(long timestamp, TDenginePrecision precision)
            {
                this._timestamp = timestamp;
                this._precision = precision;
            }

            public bool Deserialize(ITMQRows result, bool isNull, SerializationContext context)
            {
                if (isNull) return false;
                for (int col = 0; col < result.FieldCount; col++)
                {
                    Assert.False(result.IsDBNull(col));
                    var name = result.GetName(col);
                    switch (name)
                    {
                        case "ts":
                        {
                            Assert.Equal("ts", name);
                            var ts = TDengineConstant.ConvertDateTimeToTimestamp(result.GetDateTime(col), _precision);
                            Assert.Equal(_timestamp, ts);
                            result.GetDateTimeOffset(col);
                            Assert.Equal(_timestamp,
                                TDengineConstant.ConvertDateTimeOffsetToTimestamp(result.GetDateTimeOffset(col),
                                    _precision));
                            break;
                        }
                        case "c1":
                            Assert.Equal("c1", name);
                            Assert.True(result.GetBoolean(col));
                            break;
                        case "c2":
                            Assert.Equal("c2", name);
                            Assert.Equal((sbyte)2, result.GetValue(col));
                            break;
                        case "c3":
                            Assert.Equal("c3", name);
                            Assert.Equal((short)3, result.GetInt16(col));
                            break;
                        case "c4":
                            Assert.Equal("c4", name);
                            Assert.Equal(4, result.GetInt32(col));
                            break;
                        case "c5":
                            Assert.Equal("c5", name);
                            Assert.Equal((long)5, result.GetInt64(col));
                            break;
                        case "c6":
                            Assert.Equal("c6", name);
                            Assert.Equal((byte)6, result.GetByte(col));
                            break;
                        case "c7":
                            Assert.Equal("c7", name);
                            Assert.Equal((ushort)7, result.GetValue(col));
                            break;
                        case "c8":
                            Assert.Equal("c8", name);
                            Assert.Equal((uint)8, result.GetValue(col));
                            break;
                        case "c9":
                            Assert.Equal("c9", name);
                            Assert.Equal((ulong)9, result.GetValue(col));
                            break;
                        case "c10":
                            Assert.Equal("c10", name);
                            Assert.Equal((float)10, result.GetFloat(col));
                            break;
                        case "c11":
                            Assert.Equal("c11", name);
                            Assert.Equal((double)11, result.GetDouble(col));
                            break;
                        case "c12":
                        {
                            Assert.Equal("c12", name);
                            var bytes = result.GetValue(col) as byte[];
                            Assert.NotNull(bytes);
                            Assert.Equal(Encoding.UTF8.GetBytes("binary"), bytes);
                            break;
                        }
                        case "c13":
                        {
                            Assert.Equal("c13", name);
                            var nchar = result.GetString(col);
                            Assert.NotNull(nchar);
                            Assert.Equal("nchar", nchar);
                            break;
                        }
                        case "c14":
                        {
                            Assert.Equal("c14", name);
                            var bytes = result.GetValue(col) as byte[];
                            Assert.NotNull(bytes);
                            Assert.Equal(Encoding.UTF8.GetBytes("varbinary"), bytes);
                            break;
                        }
                        case "c15":
                        {
                            Assert.Equal("c15", name);
                            var point = result.GetValue(col) as byte[];
                            Assert.NotNull(point);
                            Assert.Equal(new byte[]
                            {
                                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00,
                                0x00,
                                0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                            }, point);
                            break;
                        }
                        case "c16":
                        {
                            Assert.Equal("c16", name);
                            var decimalValue = result.GetDecimal(col);
                            Assert.Equal(6581493296132535.4860m, decimalValue);
                            break;
                        }
                        case "c17":
                        {
                            Assert.Equal("c17", name);
                            var decimalValue = result.GetDecimal(col);
                            Assert.Equal(6581.4932m, decimalValue);
                            break;
                        }
                    }
                }

                return true;
            }
        }

        private void ResultTest(string connectString, string db, string topic,
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
                        "create table if not exists ct0_decimal using tmq_all_type_decimal tags(1000)",
                        "create table if not exists ct1_decimal using tmq_all_type_decimal tags(2000)",
                        "create table if not exists ct2_decimal using tmq_all_type_decimal tags(3000)",
                        $"create topic if not exists {topic} as stable tmq_all_type_decimal"
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
                            $"insert into ct{i}_decimal values('{now.ToString("yyyy-MM-dd'T'HH:mm:ss.fffK")}',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)',6581493296132535.4860,6581.4932)";
                        DoRequest(client, sql);
                    }

                    var deserializer =
                        new TestDeserializer(
                            TDengineConstant.ConvertDateTimeToTimestamp(now,
                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                            TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                    var consumer = new ConsumerBuilder<bool>(cfg).SetValueDeserializer(deserializer).Build();
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
                                Assert.True(message.Value);
                            }
                        }
                    }

                    Thread.Sleep(3000);
                    using (var result = consumer.Consume(100))
                    {
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
    }
}