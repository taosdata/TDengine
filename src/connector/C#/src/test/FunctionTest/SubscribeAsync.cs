using TDengineDriver;
using Test.UtilsTools;
using System;
using System.Collections.Generic;
using Xunit;
using Test.UtilsTools.DataSource;
using System.Threading;
using Xunit.Abstractions;
using Test.Fixture;
using Test.Case.Attributes;

namespace Cases
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]
    public class SubscribeAsyncCases
    {
        DatabaseFixture database;

        private readonly ITestOutputHelper output;

        public SubscribeAsyncCases(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this.output = output;
        }

        /// <author>xiaolei</author>
        /// <Name>SubscribeAsyncCases.ConsumeFromBegin</Name>
        /// <describe>Subscribe a table and consume through callback and the beginning record of the table</describe>
        /// <filename>Subscribe.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SubscribeAsyncCases.ConsumeFromBegin()"), TestExeOrder(1), Trait("Category", "With callback")]
        public void ConsumeFromBegin()
        {
            IntPtr conn = database.conn;
            IntPtr _res = IntPtr.Zero;

            var tableName = "subscribe_async_from_begin";
            var createSql = $"create table if not exists {tableName}(ts timestamp,bl bool,i8 tinyint,i16 smallint,i32 int,i64 bigint,bnr binary(50),nchr nchar(50))tags(t_i32 int,t_bnr binary(50),t_nchr nchar(50))";
            var dropSql = $"drop table if exists {tableName}";

            var colData = new List<Object>{1646150410100,true,1,11,1111,11111111,"value one","值壹",
            1646150410200,true,2,22,2222,22222222,"value two","值贰",
            1646150410300,false,3,33,3333,33333333,"value three","值三",
            };

            var colData2 = new List<Object>{1646150410400,false,4,44,4444,44444444,"value three","值肆",
            1646150410500,true,5,55,5555,55555555,"value one","值伍",
            1646150410600,true,6,66,6666,66666666,"value two","值陆",
            };

            var tagData = new List<Object> { 1, "tag_one", "标签壹" };
            var tagData2 = new List<Object> { 2, "tag_two", "标签贰" };

            String insertSql = UtilsTools.ConstructInsertSql(tableName + "_s01", tableName, colData, tagData, 3);
            String insertSql2 = UtilsTools.ConstructInsertSql(tableName + "_s02", tableName, colData2, tagData2, 3);
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<Object> expectResData = UtilsTools.CombineColAndTagData(colData, tagData, 3);
            List<Object> expectResData2 = UtilsTools.CombineColAndTagData(colData2, tagData2, 3);
            expectResData.AddRange(expectResData2);
            var querySql = $"select * from {tableName}";

            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);
            UtilsTools.ExecuteUpdate(conn, insertSql);

            SubscribeCallback subscribeCallback1 = new SubscribeCallback(SubCallback1);
            SubscribeCallback subscribeCallback2 = new SubscribeCallback(SubCallback2);
            IntPtr subscribe = TDengine.Subscribe(conn, true, tableName, querySql, subscribeCallback1, IntPtr.Zero, 200);

            UtilsTools.ExecuteUpdate(conn, insertSql2);
            Thread.Sleep(1000);
            TDengine.Unsubscribe(subscribe, true);

            subscribe = TDengine.Subscribe(conn, true, tableName, querySql, subscribeCallback2, IntPtr.Zero, 200);
            Thread.Sleep(1000);
            TDengine.Unsubscribe(subscribe, false);
            void SubCallback1(IntPtr subscribe, IntPtr taosRes, IntPtr param, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    // cannot free taosRes using taosRes, otherwise will cause crash.
                    UtilsTools.GetResDataWithoutFree(taosRes);
                }
                else
                {
                    output.WriteLine($"async query data failed, failed code:{code}, reason:{TDengine.Error(taosRes)}");
                }

            }

            void SubCallback2(IntPtr subscribe, IntPtr taosRes, IntPtr param, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    List<TDengineMeta> actualMeta = UtilsTools.GetResField(taosRes);
                    List<String> actualResData = UtilsTools.GetResDataWithoutFree(taosRes);
                    // UtilsTools.DisplayRes(taosRes);
                    if (actualResData.Count == 0)
                    {
                        output.WriteLine($"consume in subscribe callback without data");
                    }
                    else
                    {
                        output.WriteLine($"consume in subscribe callback with data");

                        Assert.Equal(expectResData.Count, actualResData.Count);
                        output.WriteLine("Assert Meta data");
                        //Assert Meta data
                        for (int i = 0; i < actualMeta.Count; i++)
                        {
                            Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                            Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                            Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
                        }
                        output.WriteLine("Assert retrieve data");
                        // Assert retrieve data
                        for (int i = 0; i < actualResData.Count; i++)
                        {
                            // output.WriteLine("index:{0},expectResData:{1},actualResData:{2}", i, expectResData[i], actualResData[i]);
                            Assert.Equal(expectResData[i].ToString(), actualResData[i]);
                        }
                    }
                }
                else
                {
                    output.WriteLine($"async query data failed, failed code:{code}, reason:{TDengine.Error(taosRes)}");
                }
            }

        }

        /// <author>xiaolei</author>
        /// <Name>SubscribeAsyncCases.ConsumeFromLastProgress</Name>
        /// <describe>Subscribe a table and consume through callback and from last consume progress.</describe>
        /// <filename>Subscribe.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SubscribeAsyncCases.ConsumeFromLastProgress()"), TestExeOrder(2), Trait("Category", "With callback")]
        public void ConsumeFromLastProgress()
        {
            IntPtr conn = database.conn;
            IntPtr _res = IntPtr.Zero;

            var tableName = "subscribe_async_from_begin";
            var createSql = $"create table if not exists {tableName}(ts timestamp,bl bool,i8 tinyint,i16 smallint,i32 int,i64 bigint,bnr binary(50),nchr nchar(50))tags(t_i32 int,t_bnr binary(50),t_nchr nchar(50))";
            var dropSql = $"drop table if exists {tableName}";

            var colData = new List<Object>{1646150410100,true,1,11,1111,11111111,"value one","值壹",
            1646150410200,true,2,22,2222,22222222,"value two","值贰",
            1646150410300,false,3,33,3333,33333333,"value three","值三",
            };

            var colData2 = new List<Object>{1646150410400,false,4,44,4444,44444444,"value three","值肆",
            1646150410500,true,5,55,5555,55555555,"value one","值伍",
            1646150410600,true,6,66,6666,66666666,"value two","值陆",
            };

            var tagData = new List<Object> { 1, "tag_one", "标签壹" };
            var tagData2 = new List<Object> { 2, "tag_two", "标签贰" };

            String insertSql = UtilsTools.ConstructInsertSql(tableName + "_s01", tableName, colData, tagData, 3);
            String insertSql2 = UtilsTools.ConstructInsertSql(tableName + "_s02", tableName, colData2, tagData2, 3);
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<Object> expectResData = UtilsTools.CombineColAndTagData(colData, tagData, 3);
            List<Object> expectResData2 = UtilsTools.CombineColAndTagData(colData2, tagData2, 3);
            var querySql = $"select * from {tableName}";

            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);
            UtilsTools.ExecuteUpdate(conn, insertSql);

            SubscribeCallback subscribeCallback1 = new SubscribeCallback(SubCallback1);
            SubscribeCallback subscribeCallback2 = new SubscribeCallback(SubCallback2);
            IntPtr subscribe = TDengine.Subscribe(conn, true, tableName, querySql, subscribeCallback1, IntPtr.Zero, 200);
            Thread.Sleep(1000);
            TDengine.Unsubscribe(subscribe, true);
            UtilsTools.ExecuteUpdate(conn, insertSql2);
            subscribe = TDengine.Subscribe(conn, false, tableName, querySql, subscribeCallback2, IntPtr.Zero, 200);
            Thread.Sleep(1000);
            TDengine.Unsubscribe(subscribe, false);
            void SubCallback1(IntPtr subscribe, IntPtr taosRes, IntPtr param, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    // cannot free taosRes using taosRes, otherwise will cause crash.
                    UtilsTools.GetResDataWithoutFree(taosRes);
                }
                else if (taosRes != IntPtr.Zero)
                {
                    output.WriteLine($"async query data failed, failed code:{code}, reason:{TDengine.Error(taosRes)}");
                }

            }

            void SubCallback2(IntPtr subscribe, IntPtr taosRes, IntPtr param, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    List<TDengineMeta> actualMeta = UtilsTools.GetResField(taosRes);
                    List<String> actualResData = UtilsTools.GetResDataWithoutFree(taosRes);
                    UtilsTools.DisplayRes(taosRes);
                    if (actualResData.Count == 0)
                    {
                        output.WriteLine($"consume in subscribe callback without data");
                    }
                    else
                    {
                        output.WriteLine($"consume in subscribe callback with data");

                        Assert.Equal(expectResData2.Count, actualResData.Count);
                        output.WriteLine("Assert Meta data");
                        //Assert Meta data
                        for (int i = 0; i < actualMeta.Count; i++)
                        {
                            Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                            Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                            Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
                        }
                        output.WriteLine("Assert retrieve data");
                        // Assert retrieve data
                        for (int i = 0; i < actualResData.Count; i++)
                        {
                            // output.WriteLine("index:{0},expectResData:{1},actualResData:{2}", i, expectResData[i], actualResData[i]);
                            Assert.Equal(expectResData2[i].ToString(), actualResData[i]);
                        }
                    }
                }
                else
                {
                    output.WriteLine($"async query data failed, failed code:{code}, reason:{TDengine.Error(taosRes)}");
                }
            }

        }
    }
}