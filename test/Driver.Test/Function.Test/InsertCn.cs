using System;
using System.Collections.Generic;
using System.Text;
using TDengine.Driver;
using Test.Case.Attributes;
using Test.Fixture;
using Test.Utils;
using Test.Utils.ResultSet;
using Xunit;
using Xunit.Abstractions;

namespace Function.Test.Taosc
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]
    public class InsertCNCases
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;

        public InsertCNCases(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }

        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestNTable</Name>
        /// <describe>Test insert Chinese characters into normal table's nchar column</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "InsertCNCases.TestNTable"), TestExeOrder(1)]
        public void TestNTable()
        {
            IntPtr conn = database.Conn;
            Assert.NotEqual(conn, IntPtr.Zero);
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_insert_nchar_ntable";
            var now = DateTime.Now;
            var ts = TDengineConstant.ConvertDatetimeToTick(now, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            now = TDengineConstant.ConvertTimeToDatetime(ts, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            var colData = new List<Object>
            {
                now, 1, "涛思数据",
                now.Add(TimeSpan.FromSeconds(1)), 2, "涛思数据taosdata",
                now.Add(TimeSpan.FromSeconds(2)), 3, "TDegnine涛思数据",
                now.Add(TimeSpan.FromSeconds(3)), 4, "4涛思数据",
                now.Add(TimeSpan.FromSeconds(4)), 5, "涛思数据5",
                now.Add(TimeSpan.FromSeconds(5)), 6, "taos涛思数据6",
                now.Add(TimeSpan.FromSeconds(6)), 7, "7涛思数据taos",
                now.Add(TimeSpan.FromSeconds(7)), 8, "8&涛思数据taos",
                now.Add(TimeSpan.FromSeconds(8)), 9, "&涛思数据taos9"
            };

            String dropTb = $"drop table if exists {tableName}";
            String createTb = $"create table if not exists {tableName} (ts timestamp,v4 int,nchr nchar(200));";
            String insertSql = Tools.ConstructInsertSql(tableName, "", colData, null, 9);
            String selectSql = $"select * from {tableName}";
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTb);

            Tools.ExecuteUpdate(conn, dropTb, _output);
            Tools.ExecuteUpdate(conn, createTb, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);
            _res = Tools.ExecuteQuery(conn, selectSql, _output);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            //Assert Meta data
            _output.WriteLine("Assert Meta data");
            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");
            for (int i = 0; i < actualResData.Count; i++)
            {
                // _output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(colData[i], actualResData[i]);
            }

            _output.WriteLine("InsertCNCases.TestNTable() pass");
        }

        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestSTable</Name>
        /// <describe>test insert Chinese character into stable's nchar column,both tag and column</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "InsertCNCases.TestSTable()"), TestExeOrder(2)]
        public void TestSTable()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_insert_nchar_stable";
            var now = DateTime.Now;
            var ts = TDengineConstant.ConvertDatetimeToTick(now, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            now = TDengineConstant.ConvertTimeToDatetime(ts, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            var colData = new List<Object>
            {
                now, 1, "涛思数据",
                now.Add(TimeSpan.FromSeconds(1)), 2, "涛思数据taosdata",
                now.Add(TimeSpan.FromSeconds(2)), 3, "TDegnine涛思数据",
                now.Add(TimeSpan.FromSeconds(3)), 4, "4涛思数据",
                now.Add(TimeSpan.FromSeconds(4)), 5, "涛思数据5",
                now.Add(TimeSpan.FromSeconds(5)), 6, "taos涛思数据6",
                now.Add(TimeSpan.FromSeconds(6)), 7, "7涛思数据taos",
                now.Add(TimeSpan.FromSeconds(7)), 8, "8&涛思数据taos",
                now.Add(TimeSpan.FromSeconds(8)), 9, "&涛思数据taos9"
            };
            var tagData = new List<Object> { 1, "涛思数据", };
            String dropTb = "drop table if exists " + tableName;
            String createTb =
                $"create table {tableName} (ts timestamp,v4 int,nchr nchar(200))tags(id int,name nchar(50));";
            String insertSql = Tools.ConstructInsertSql(tableName + "_sub1", tableName, colData, tagData, 9);
            String selectSql = $"select * from {tableName}";
            String dropSql = $"drop table {tableName}";
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTb);

            List<Object> expectResData = Tools.ConstructResData(colData, tagData, 9);

            Tools.ExecuteUpdate(conn, dropTb, _output);
            Tools.ExecuteUpdate(conn, createTb, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);
            _res = Tools.ExecuteQuery(conn, selectSql, _output);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            //Assert Meta data
            _output.WriteLine("Assert Meta data");
            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < actualResData.Count; i++)
            {
                // _output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                Assert.Equal(expectResData[i], actualResData[i]);
            }

            _output.WriteLine("InsertCNCases.TestSTable() pass");
        }

        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestInsertMultiNTable</Name>
        /// <describe>test insert Chinese character into normal table's multiple nchar columns</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "InsertCNCases.TestInsertMultiNTable()"), TestExeOrder(3)]
        public void TestInsertMultiNTable()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_multi_insert_nchar_ntable";
            var now = DateTime.Now;
            var ts = TDengineConstant.ConvertDatetimeToTick(now, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            now = TDengineConstant.ConvertTimeToDatetime(ts, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            var colData = new List<Object>
            {
                now, 1, "涛思数据", "保利广场", "Beijing", "China",
                now.Add(TimeSpan.FromSeconds(1)), 2, "涛思数据taosdata", "保利广场baoli", "Beijing", "China",
                now.Add(TimeSpan.FromSeconds(2)), 3, "TDegnine涛思数据", "time广场", "NewYork", "US",
                now.Add(TimeSpan.FromSeconds(3)), 4, "4涛思数据", "4广场南部", "London", "UK",
                now.Add(TimeSpan.FromSeconds(4)), 5, "涛思数据5", "!广场路中部123", "Tokyo", "JP",
                now.Add(TimeSpan.FromSeconds(5)), 6, "taos涛思数据6", "青年广场123号！", "Washin", "DC",
                now.Add(TimeSpan.FromSeconds(6)), 7, "7涛思数据taos", "asdf#壮年广场%#endregion", "NewYork", "US",
                now.Add(TimeSpan.FromSeconds(7)), 8, "8&涛思数据taos", "include阿斯顿发", "NewYork", "US",
                now.Add(TimeSpan.FromSeconds(8)), 9, "&涛思数据taos9", "123黑化肥werq会挥……&¥%发！afsdfa", "NewYork", "US",
            };

            String dropTb = "drop table if exists " + tableName;
            String createTb =
                $"create table if not exists {tableName} (ts timestamp,v4 int,c_nchar nchar(200),location nchar(200),city binary(100),coutry binary(200));";
            String insertSql = Tools.ConstructInsertSql(tableName, "", colData, null, 9);
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTb);

            Tools.ExecuteUpdate(conn, dropTb, _output);
            Tools.ExecuteUpdate(conn, createTb, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);
            _res = Tools.ExecuteQuery(conn, selectSql, _output);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;

            //Assert Meta data
            _output.WriteLine("Assert Meta data");

            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < actualResData.Count; i++)
            {
                // _output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                switch (actualResData[i])
                {
                    case byte[] val:
                        Assert.Equal(colData[i], Encoding.UTF8.GetString(val));
                        break;
                    default:
                        Assert.Equal(colData[i], actualResData[i]);
                        break;
                }
            }

            _output.WriteLine("InsertCNCases.TestInsertMultiNTable() passed");
        }

        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestInsertMultiSTable</Name>
        /// <describe>test insert Chinese character into stable's multiple nchar columns</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "InsertCNCases.TestInsertMultiSTable()"), TestExeOrder(4)]
        public void TestInsertMultiSTable()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_multi_insert_nchar_stable";
            var now = DateTime.Now;
            var ts = TDengineConstant.ConvertDatetimeToTick(now, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            now = TDengineConstant.ConvertTimeToDatetime(ts, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            var colData = new List<Object>
            {
                now, 1, "涛思数据", "保利广场", "Beijing", "China",
                now.Add(TimeSpan.FromSeconds(1)), 2, "涛思数据taosdata", "保利广场baoli", "Beijing", "China",
                now.Add(TimeSpan.FromSeconds(2)), 3, "TDegnine涛思数据", "time广场", "NewYork", "US",
                now.Add(TimeSpan.FromSeconds(3)), 4, "4涛思数据", "4广场南部", "London", "UK",
                now.Add(TimeSpan.FromSeconds(4)), 5, "涛思数据5", "!广场路中部123", "Tokyo", "JP",
                now.Add(TimeSpan.FromSeconds(5)), 6, "taos涛思数据6", "青年广场123号！", "Washin", "DC",
                now.Add(TimeSpan.FromSeconds(6)), 7, "7涛思数据taos", "asdf#壮年广场%#endregion", "NewYork", "US",
                now.Add(TimeSpan.FromSeconds(7)), 8, "8&涛思数据taos", "include阿斯顿发", "NewYork", "US",
                now.Add(TimeSpan.FromSeconds(8)), 9, "&涛思数据taos9", "123黑化肥werq会挥……&¥%发！afsdfa", "NewYork", "US",
            };
            var tagData = new List<Object> { 1, "涛思数据", "中国北方&南方长江黄河！49wq", "tdengine" };
            String dropTb = "drop table if exists " + tableName;
            String createTb = $"create table if not exists {tableName} (ts timestamp," +
                              $"v4 int," +
                              $"c_nchar nchar(200)," +
                              $"locate nchar(200)," +
                              $"country nchar(200)," +
                              $"city nchar(50)" +
                              $")tags(" +
                              $"id int," +
                              $"name nchar(50)," +
                              $"addr nchar(200)," +
                              $"en_name binary(200));";
            String insertSql = Tools.ConstructInsertSql(tableName + "_sub1", tableName, colData, tagData, 9);
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTb);

            List<Object> expectResData = Tools.ConstructResData(colData, tagData, 9);

            Tools.ExecuteUpdate(conn, dropTb, _output);
            Tools.ExecuteUpdate(conn, createTb, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);
            _res = Tools.ExecuteQuery(conn, selectSql, _output);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.ResultMeta;
            List<Object> actualResData = actualResult.ResultData;
            //Assert Meta data
            _output.WriteLine("Assert Meta data");

            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            _output.WriteLine("Assert retrieve data");

            for (int i = 0; i < actualResData.Count; i++)
            {
                //_output.WriteLine("expect:{0},actual:{1}", expectResData[i], actualResData[i]);
                switch (actualResData[i])
                {
                    case byte[] val:
                        Assert.Equal(expectResData[i], Encoding.UTF8.GetString(val));
                        break;
                    default:
                        Assert.Equal(expectResData[i], actualResData[i]);
                        break;
                }
                // Assert.Equal(expectResData[i], actualResData[i]);
            }

            _output.WriteLine("InsertCNCases.TestInsertMultiSTable() pass");
        }
    }
}