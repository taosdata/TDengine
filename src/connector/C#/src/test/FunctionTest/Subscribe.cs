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

    public class SubscribeCases
    {
        DatabaseFixture database;

        private readonly ITestOutputHelper output;

        public SubscribeCases(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this.output = output;
        }
        /// <author>xiaolei</author>
        /// <Name>SubscribeCases.ConsumeFromBegin</Name>
        /// <describe>Subscribe a table and consume from beginning.</describe>
        /// <filename>Subscribe.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SubscribeCases.ConsumeFromBegin()"), TestExeOrder(1), Trait("Category", "Without callback")]
        public void ConsumeFromBegin()
        {
            IntPtr conn = database.conn;
            IntPtr _res = IntPtr.Zero;

            var tableName = "subscribe_from_begin";
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
            // Then
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<Object> expectResData = UtilsTools.CombineColAndTagData(colData, tagData, 3);
            List<Object> expectResData2 = UtilsTools.CombineColAndTagData(colData2, tagData2, 3);
            expectResData.AddRange(expectResData2);

            var querySql = $"select * from {tableName}";
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);
            UtilsTools.ExecuteUpdate(conn, insertSql);


            IntPtr subscribe = TDengine.Subscribe(conn, true, tableName, querySql, null, IntPtr.Zero, 0);
            _res = TDengine.Consume(subscribe);
            // need to call fetch TAOS_RES
            UtilsTools.GetResDataWithoutFree(_res);
            TDengine.Unsubscribe(subscribe, true);

            UtilsTools.ExecuteUpdate(conn, insertSql2);
            Thread.Sleep(100);


            subscribe = TDengine.Subscribe(conn, true, tableName, querySql, null, IntPtr.Zero, 0);
            _res = TDengine.Consume(subscribe);

            List<TDengineMeta> actualMeta = UtilsTools.GetResField(_res);
            List<String> actualResData = UtilsTools.GetResDataWithoutFree(_res);
            TDengine.Unsubscribe(subscribe, false);

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
                // output.WriteLine("{0},{1},{2}", i, expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i].ToString(), actualResData[i]);
            }

        }

        /// <author>xiaolei</author>
        /// <Name>SubscribeCases.ConsumeFromLastProgress</Name>
        /// <describe>Subscribe table from the last progress.</describe>
        /// <filename>Subscribe.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "SubscribeCases.ConsumeFromLastProgress()"), TestExeOrder(2), Trait("Category", "Without callback")]
        public void ConsumeFromLastProgress()
        {
            IntPtr conn = database.conn;
            IntPtr _res = IntPtr.Zero;

            var tableName = "subscribe_from_progress";
            var createSql = $"create table if not exists {tableName}(ts timestamp,bl bool,i8 tinyint,i16 smallint,i32 int,i64 bigint,bnr binary(50),nchr nchar(50))tags(t_i32 int,t_bnr binary(50),t_nchr nchar(50))";
            var dropSql = $"drop table if exists {tableName}";

            var colData = new List<Object>{1646150410100,true,1,11,1111,11111111,"value one","值壹",
            1646150410200,true,2,22,2222,22222222,"value two","值贰",
            1646150410300,false,3,33,3333,33333333,"value three","值叁",
            };

            var colData2 = new List<Object>{1646150410400,false,4,44,4444,44444444,"value three","值肆",
            1646150410500,true,5,55,5555,55555555,"value one","值伍",
            1646150410600,true,6,66,6666,66666666,"value two","值陆",
            };

            var tagData = new List<Object> { 1, "tag_one", "标签壹" };
            var tagData2 = new List<Object> { 2, "tag_two", "标签贰" };

            String insertSql = UtilsTools.ConstructInsertSql(tableName + "_s01", tableName, colData, tagData, 3);
            String insertSql2 = UtilsTools.ConstructInsertSql(tableName + "_s02", tableName, colData2, tagData2, 3);
            // Then
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<Object> expectResData = UtilsTools.CombineColAndTagData(colData, tagData, 3);
            List<Object> expectResData2 = UtilsTools.CombineColAndTagData(colData2, tagData2, 3);


            var querySql = $"select * from {tableName}";
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);
            UtilsTools.ExecuteUpdate(conn, insertSql);

            // First time subscribe
            IntPtr subscribe = TDengine.Subscribe(conn, true, tableName, querySql, null, IntPtr.Zero, 20);
            _res = TDengine.Consume(subscribe);
            // need to call fetch TAOS_RES
            UtilsTools.GetResDataWithoutFree(_res);
            // Close subscribe and save progress.
            TDengine.Unsubscribe(subscribe, true);

            // Insert new data.
            UtilsTools.ExecuteUpdate(conn, insertSql2);
            Thread.Sleep(1000);

            subscribe = TDengine.Subscribe(conn, false, tableName, querySql, null, IntPtr.Zero, 20);
            _res = TDengine.Consume(subscribe);

            List<TDengineMeta> actualMeta = UtilsTools.GetResField(_res);
            List<String> actualResData = UtilsTools.GetResDataWithoutFree(_res);
            TDengine.Unsubscribe(subscribe, true);
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
                // output.WriteLine("{0},{1},{2}", i, expectResData[i], actualResData[i]);
                Assert.Equal(expectResData2[i].ToString(), actualResData[i]);
            }

        }
    }

}