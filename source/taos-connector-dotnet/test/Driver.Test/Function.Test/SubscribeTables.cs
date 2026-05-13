using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.TMQ;
using Test.Case.Attributes;
using Test.Fixture;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Function.Test.TMQ
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]
    public class SubscribeTable
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;

        public SubscribeTable(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }

        /// <author>xiaolei</author>
        /// <Name>SubscribeTable.NormalTable</Name>
        /// <describe>Subscribe a table and consume from beginning.</describe>
        /// <filename>SubscribeTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SubscribeTable.NormalTable()"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            IntPtr conn = database.Conn;
            string tableName = "tmq_tb0";
            string topic = "topic_t0";
            string createSql = Tools.CreateTable(tableName, false, false);
            string createTopic = $"create topic if not exists {topic} as select * from {tableName}";
            string dropTopic = $"drop topic if exists {topic}";

            List<object> columns = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1);
            string insertSql = Tools.ConstructInsertSql($"{tableName}", "", columns, tags, 5);

            List<object> expectResData = Tools.ConstructResData(columns, tags, 5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);

            var cfg = new ConsumerConfig
            {
                GroupId = "c#_1",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = "true",
                TDConnectIp = "127.0.0.1",
                EnableAutoCommit = "true",
            };

            // prepare data
            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            // create topic 
            Tools.ExecuteUpdate(conn, createTopic, _output);

            // consumer
            var consumerBuilder = new ConsumerBuilder<TMQResult>(cfg);
            consumerBuilder.SetValueDeserializer(new ReferenceDeserializer<TMQResult>());
            var consumer = consumerBuilder.Build();

            consumer.Subscribe(topic);
            List<string> subTopics = consumer.Subscription();

            Assert.Equal(topic, subTopics[0]);

            for (int i = 0; i < 5; i++)
            {
                using (ConsumeResult<TMQResult> consumerResult = consumer.Consume(200))
                {
                    if (consumerResult == null)
                    {
                        _output.WriteLine(" ======= consume {0} done", i);
                        continue;
                    }

                    for (int j = 0; j < consumerResult.Message.Count; j++)
                    {
                        var b = j * 27;
                        var v = consumerResult.Message[j].Value;
                        _output.WriteLine(" ======= consume {0} {1}", i, tableName);
                        var ts = TDengineConstant.ConvertDateTimeToTimestamp(v.ts,
                            TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                        Assert.Equal(expectResData[0 + b], ts);
                        Assert.Equal(expectResData[1 + b], v.v1);
                        Assert.Equal(expectResData[2 + b], v.v2);
                        Assert.Equal(expectResData[3 + b], v.v4);
                        Assert.Equal(expectResData[4 + b], v.v8);
                        Assert.Equal(expectResData[5 + b], v.u1);
                        Assert.Equal(expectResData[6 + b], v.u2);
                        Assert.Equal(expectResData[7 + b], v.u4);
                        Assert.Equal(expectResData[8 + b], v.u8);
                        Assert.Equal(expectResData[9 + b], v.f4);
                        Assert.Equal(expectResData[10 + b], v.f8);
                        Assert.Equal(expectResData[11 + b], v.bin);
                        Assert.Equal(expectResData[12 + b], v.nchr);
                        Assert.Equal(expectResData[13 + b], v.b);
                    }

                    consumer.Commit(consumerResult);
                    _output.WriteLine(" ======= consume {0} done", i);
                }
            }

            // dispose
            consumer.Unsubscribe();
            Tools.ExecuteUpdate(conn, dropTopic, _output);
            consumer.Close();
        }

        /// <author>xiaolei</author>
        /// <Name>SubscribeCases.ConsumeFromLastProgress</Name>
        /// <describe>Subscribe table from the last progress.</describe>
        /// <filename>Subscribe.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SubscribeTable.MultiTables()"), TestExeOrder(2), Trait("Category", "MultiTables")]
        public void MultiTables()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;

            string[] tableName = new string[] { "tmq_tb1", "tmq_tb2", "tmq_jb1" };
            string[] topic = new string[] { "topic_t1", "topic_t2", "topic_j1" };

            string[] createSql = new string[3];
            string[] createTopic = new string[3];
            string[] insertSql = new string[3];
            string[] dropTopic = new string[3];
            List<object> columns = Tools.ColumnsList(1);

            List<List<object>> expectResData = new List<List<object>>(3);
            List<List<TDengineMeta>> expectResMeta = new List<List<TDengineMeta>>(3);
            List<List<object>> tags = new List<List<object>>(3);

            // prepare SQL

            // table 

            tags.Add(new List<object>());
            createSql[0] = Tools.CreateTable(tableName[0], false, false);
            insertSql[0] = Tools.ConstructInsertSql(tableName[0], "", columns, null, 1);
            expectResData.Add(columns);
            expectResMeta.Add(Tools.GetMetaFromDDL(createSql[0]));

            // stable
            tags.Add(Tools.TagsList(2, false));
            createSql[1] = Tools.CreateTable(tableName[1], true, false);
            insertSql[1] =
                Tools.ConstructInsertSql($"{tableName[1]}_s1", tableName[1], columns, tags[1], 1);
            expectResData.Add(Tools.ConstructResData(columns, tags[1], 1));
            expectResMeta.Add(Tools.GetMetaFromDDL(createSql[1]));
            // JSON
            tags.Add(Tools.TagsList(3, true));
            createSql[2] = Tools.CreateTable(tableName[2], true, true);
            insertSql[2] =
                Tools.ConstructInsertSql($"{tableName[2]}_j1", tableName[2], columns, tags[2], 1);
            expectResData.Add(Tools.ConstructResData(columns, tags[2], 1));
            expectResMeta.Add(Tools.GetMetaFromDDL(createSql[2]));

            for (int i = 0; i < 3; i++)
            {
                createTopic[i] = $"create topic if not exists {topic[i]} as select * from {tableName[i]}";
                dropTopic[i] = $"drop topic if exists {topic[i]}";
            }

            // create table,topic
            for (int i = 0; i < 3; i++)
            {
                Tools.ExecuteUpdate(conn, createSql[i], _output);
                Tools.ExecuteUpdate(conn, insertSql[i], _output);
                Tools.ExecuteUpdate(conn, createTopic[i], _output);
            }

            var cfg = new ConsumerConfig
            {
                GroupId = "c#_2",
                TDConnectUser = "root",
                TDConnectPasswd = "taosdata",
                MsgWithTableName = "true",
                TDConnectIp = "127.0.0.1",
                TDConnectPort = "6030",
                EnableAutoCommit = "true",
                ClientId = "test_client",
                AutoCommitIntervalMs = "3000",
                SessionTimeoutMs = "12000",
                MaxPollIntervalMs = "300000"
            };


            // consumer
            var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
            consumer.Subscribe(topic);
            List<string> subTopics = consumer.Subscription();

            subTopics.ForEach(tpc => { Assert.Contains(tpc, topic); });

            for (int i = 0; i < 5; i++)
            {
                using (var consumeResult = consumer.Consume(200))
                {
                    if (consumeResult == null)
                    {
                        _output.WriteLine("======= consume {0} done", i);
                        continue;
                    }

                    {
                        switch (consumeResult.Topic)
                        {
                            // if normal table 
                            case "topic_t1":
                                _output.WriteLine("tmq_tb1");
                                for (int j = 0; j < consumeResult.Message.Count; j++)
                                {
                                    var v = consumeResult.Message[j].Value;
                                    var ts = TDengineConstant.ConvertDateTimeToTimestamp((DateTime)v["ts"],
                                        TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                    Assert.Equal(expectResData[0][0], ts);
                                    Assert.Equal(expectResData[0][1], v["v1"]);
                                    Assert.Equal(expectResData[0][2], v["v2"]);
                                    Assert.Equal(expectResData[0][3], v["v4"]);
                                    Assert.Equal(expectResData[0][4], v["v8"]);
                                    Assert.Equal(expectResData[0][5], v["u1"]);
                                    Assert.Equal(expectResData[0][6], v["u2"]);
                                    Assert.Equal(expectResData[0][7], v["u4"]);
                                    Assert.Equal(expectResData[0][8], v["u8"]);
                                    Assert.Equal(expectResData[0][9], v["f4"]);
                                    Assert.Equal(expectResData[0][10], v["f8"]);
                                    Assert.Equal(expectResData[0][11], v["bin"]);
                                    Assert.Equal(expectResData[0][12], v["nchr"]);
                                    Assert.Equal(expectResData[0][13], v["b"]);
                                }

                                break;
                            // if stable
                            case "topic_t2":
                                _output.WriteLine("tmq_tb2_s1");
                                for (int j = 0; j < consumeResult.Message.Count; j++)
                                {
                                    var v = consumeResult.Message[j].Value;
                                    var ts = TDengineConstant.ConvertDateTimeToTimestamp((DateTime)v["ts"],
                                        TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                    Assert.Equal(expectResData[1][0], ts);
                                    Assert.Equal(expectResData[1][1], v["v1"]);
                                    Assert.Equal(expectResData[1][2], v["v2"]);
                                    Assert.Equal(expectResData[1][3], v["v4"]);
                                    Assert.Equal(expectResData[1][4], v["v8"]);
                                    Assert.Equal(expectResData[1][5], v["u1"]);
                                    Assert.Equal(expectResData[1][6], v["u2"]);
                                    Assert.Equal(expectResData[1][7], v["u4"]);
                                    Assert.Equal(expectResData[1][8], v["u8"]);
                                    Assert.Equal(expectResData[1][9], v["f4"]);
                                    Assert.Equal(expectResData[1][10], v["f8"]);
                                    Assert.Equal(expectResData[1][11], v["bin"]);
                                    Assert.Equal(expectResData[1][12], v["nchr"]);
                                    Assert.Equal(expectResData[1][13], v["b"]);
                                    Assert.Equal(expectResData[1][14], v["bo"]);
                                    Assert.Equal(expectResData[1][15], v["tt"]);
                                    Assert.Equal(expectResData[1][16], v["si"]);
                                    Assert.Equal(expectResData[1][17], v["ii"]);
                                    Assert.Equal(expectResData[1][18], v["bi"]);
                                    Assert.Equal(expectResData[1][19], v["tu"]);
                                    Assert.Equal(expectResData[1][20], v["su"]);
                                    Assert.Equal(expectResData[1][21], v["iu"]);
                                    Assert.Equal(expectResData[1][22], v["bu"]);
                                    Assert.Equal(expectResData[1][23], v["ff"]);
                                    Assert.Equal(expectResData[1][24], v["dd"]);
                                    Assert.Equal(expectResData[1][25], v["bb"]);
                                    Assert.Equal(expectResData[1][26], v["nc"]);
                                }

                                break;
                            // if JSON table
                            case "topic_j1":
                                _output.WriteLine("tmq_jb1_j1");
                                for (int j = 0; j < consumeResult.Message.Count; j++)
                                {
                                    var v = consumeResult.Message[j].Value;
                                    var ts = TDengineConstant.ConvertDateTimeToTimestamp((DateTime)v["ts"],
                                        TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                                    Assert.Equal(expectResData[2][0], ts);
                                    Assert.Equal(expectResData[2][1], v["v1"]);
                                    Assert.Equal(expectResData[2][2], v["v2"]);
                                    Assert.Equal(expectResData[2][3], v["v4"]);
                                    Assert.Equal(expectResData[2][4], v["v8"]);
                                    Assert.Equal(expectResData[2][5], v["u1"]);
                                    Assert.Equal(expectResData[2][6], v["u2"]);
                                    Assert.Equal(expectResData[2][7], v["u4"]);
                                    Assert.Equal(expectResData[2][8], v["u8"]);
                                    Assert.Equal(expectResData[2][9], v["f4"]);
                                    Assert.Equal(expectResData[2][10], v["f8"]);
                                    Assert.Equal(expectResData[2][11], v["bin"]);
                                    Assert.Equal(expectResData[2][12], v["nchr"]);
                                    Assert.Equal(expectResData[2][13], v["b"]);
                                    Assert.Equal(expectResData[2][14], v["json_tag"]);
                                }

                                break;
                            default:
                                throw new Exception($"Unexpected table name {consumeResult.Topic}");
                        }
                    }

                    consumer.Commit(consumeResult);

                    _output.WriteLine("======= consume {0} done", i);
                }
            }
            // assert 

            // dispose
            consumer.Unsubscribe();
            foreach (var s in dropTopic)
            {
                Tools.ExecuteUpdate(conn, s, _output);
            }

            consumer.Close();
        }
    }

    public class TMQResult
    {
        public DateTime ts { get; set; }
        public sbyte v1 { get; set; }
        public short v2 { get; set; }
        public int v4 { get; set; }
        public long v8 { get; set; }
        public byte u1 { get; set; }
        public ushort u2 { get; set; }
        public uint u4 { get; set; }
        public ulong u8 { get; set; }
        public float f4 { get; set; }
        public double f8 { get; set; }
        public byte[] bin { get; set; }
        public string nchr { get; set; }
        public bool b { get; set; }
    }
}