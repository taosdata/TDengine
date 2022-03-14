using System;
using Test.UtilsTools;
using TDengineDriver;
using Test.UtilsTools.DataSource;
using Xunit;
using System.Collections.Generic;
using Test.UtilsTools.ResultSet;
namespace Cases
{
    public class InsertCNCases
    {
        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestNTable</Name>
        /// <describe>Test insert Chinese characters into normal table's nchar column</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "InsertCNCases.TestNTable()")]
        public void TestNTable()
        {
            IntPtr conn = UtilsTools.TDConnection();
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_insert_nchar_ntable";
            // var expectResData = new List<String> { "1637064040000", "true", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "XI", "XII", "{\"k1\": \"v1\"}" };
            var colData = new List<Object>{1637064040000,1,"涛思数据",
            1637064041000,2,"涛思数据taosdata",
            1637064042000,3,"TDegnine涛思数据",
            1637064043000,4,"4涛思数据",
            1637064044000,5,"涛思数据5",
            1637064045000,6,"taos涛思数据6",
            1637064046000,7,"7涛思数据taos",
            1637064047000,8,"8&涛思数据taos",
            1637064048000,9,"&涛思数据taos9"
            };

            String dropTb = "drop table if exists " + tableName;
            String createTb = $"create table if not exists {tableName} (ts timestamp,v4 int,blob nchar(200));";
            String insertSql = UtilsTools.ConstructInsertSql(tableName, "", colData, null, 9);
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            UtilsTools.ExecuteUpdate(conn, dropTb);
            UtilsTools.ExecuteUpdate(conn, createTb);
            UtilsTools.ExecuteUpdate(conn, insertSql);
            _res = UtilsTools.ExecuteQuery(conn, selectSql);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.GetResultMeta();
            List<String> actualResData = actualResult.GetResultData();
            //Assert Meta data
            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(colData[i].ToString(), actualResData[i]);
            }

        }

        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestSTable</Name>
        /// <describe>test insert Chinese character into stable's nchar column,both tag and column</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "InsertCNCases.TestSTable()")]
        public void TestSTable()
        {
            IntPtr conn = UtilsTools.TDConnection();
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_insert_nchar_stable";
            var colData = new List<Object>{1637064040000,1,"涛思数据",
            1637064041000,2,"涛思数据taosdata",
            1637064042000,3,"TDegnine涛思数据",
            1637064043000,4,"4涛思数据",
            1637064044000,5,"涛思数据5",
            1637064045000,6,"taos涛思数据6",
            1637064046000,7,"7涛思数据taos",
            1637064047000,8,"8&涛思数据taos",
            1637064048000,9,"&涛思数据taos9"
            };
            var tagData = new List<Object> { 1, "涛思数据", };
            String dropTb = "drop table if exists " + tableName;
            String createTb = $"create table {tableName} (ts timestamp,v4 int,blob nchar(200))tags(id int,name nchar(50));";
            String insertSql = UtilsTools.ConstructInsertSql(tableName + "_sub1", tableName, colData, tagData, 9);
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            List<Object> expectResData = UtilsTools.CombineColAndTagData(colData, tagData, 9);

            UtilsTools.ExecuteUpdate(conn, dropTb);
            UtilsTools.ExecuteUpdate(conn, createTb);
            UtilsTools.ExecuteUpdate(conn, insertSql);
            _res = UtilsTools.ExecuteQuery(conn, selectSql);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.GetResultMeta();
            List<String> actualResData = actualResult.GetResultData();
            //Assert Meta data
            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i].ToString(), actualResData[i]);
            }
        }

        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestInsertMultiNTable</Name>
        /// <describe>test insert Chinese character into normal table's multiple nchar columns</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "InsertCNCases.TestInsertMultiNTable()")]
        public void TestInsertMultiNTable()
        {
            IntPtr conn = UtilsTools.TDConnection();
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_multi_insert_nchar_ntable";
            var colData = new List<Object>{1637064040000,1,"涛思数据","保利广场","Beijing","China",
            1637064041000,2,"涛思数据taosdata","保利广场baoli","Beijing","China",
            1637064042000,3,"TDegnine涛思数据","time广场","NewYork","US",
            1637064043000,4,"4涛思数据","4广场南部","London","UK",
            1637064044000,5,"涛思数据5","!广场路中部123","Tokyo","JP",
            1637064045000,6,"taos涛思数据6","青年广场123号！","Washin","DC",
            1637064046000,7,"7涛思数据taos","asdf#壮年广场%#endregion","NewYork","US",
            1637064047000,8,"8&涛思数据taos","incluse阿斯顿发","NewYork","US",
            1637064048000,9,"&涛思数据taos9","123黑化肥werq会挥……&¥%发！afsdfa","NewYork","US",
            };

            String dropTb = "drop table if exists " + tableName;
            String createTb = $"create table if not exists {tableName} (ts timestamp,v4 int,blob nchar(200),location nchar(200),city binary(100),country binary(200));";
            String insertSql = UtilsTools.ConstructInsertSql(tableName, "", colData, null, 9);
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            UtilsTools.ExecuteUpdate(conn, dropTb);
            UtilsTools.ExecuteUpdate(conn, createTb);
            UtilsTools.ExecuteUpdate(conn, insertSql);
            _res = UtilsTools.ExecuteQuery(conn, selectSql);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.GetResultMeta();
            List<String> actualResData = actualResult.GetResultData();
            //Assert Meta data
            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(colData[i].ToString(), actualResData[i]);
            }
        }

        /// <author>xiaolei</author>
        /// <Name>InsertCNCases.TestInsertMultiSTable</Name>
        /// <describe>test insert Chinese character into stable's multiple nchar columns</describe>
        /// <filename>InsertCn.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "InsertCNCases.TestInsertMultiSTable()")]
        public void TestInsertMultiSTable()
        {
            IntPtr conn = UtilsTools.TDConnection();
            IntPtr _res = IntPtr.Zero;
            string tableName = "cn_multi_insert_nchar_stable";
            var colData = new List<Object>{1637064040000,1,"涛思数据","保利广场","Beijing","China",
            1637064041000,2,"涛思数据taosdata","保利广场baoli","Beijing","China",
            1637064042000,3,"TDegnine涛思数据","time广场","NewYork","US",
            1637064043000,4,"4涛思数据","4广场南部","London","UK",
            1637064044000,5,"涛思数据5","!广场路中部123","Tokyo","JP",
            1637064045000,6,"taos涛思数据6","青年广场123号！","Washin","DC",
            1637064046000,7,"7涛思数据taos","asdf#壮年广场%#endregion","NewYork","US",
            1637064047000,8,"8&涛思数据taos","incluse阿斯顿发","NewYork","US",
            1637064048000,9,"&涛思数据taos9","123黑化肥werq会挥……&¥%发！afsdfa","NewYork","US",
            };
            var tagData = new List<Object> { 1, "涛思数据", "中国北方&南方长江黄河！49wq", "tdengine" };
            String dropTb = "drop table if exists " + tableName;
            String createTb = $"create table if not exists {tableName} (ts timestamp," +
            $"v4 int," +
            $"blob nchar(200)," +
            $"locate nchar(200)," +
            $"country nchar(200)," +
            $"city nchar(50)" +
            $")tags(" +
            $"id int," +
            $"name nchar(50)," +
            $"addr nchar(200)," +
            $"en_name binary(200));";
            String insertSql = UtilsTools.ConstructInsertSql(tableName + "_sub1", tableName, colData, tagData, 9);
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            List<Object> expectResData = UtilsTools.CombineColAndTagData(colData, tagData, 9);

            UtilsTools.ExecuteUpdate(conn, dropTb);
            UtilsTools.ExecuteUpdate(conn, createTb);
            UtilsTools.ExecuteUpdate(conn, insertSql);
            _res = UtilsTools.ExecuteQuery(conn, selectSql);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.GetResultMeta();
            List<String> actualResData = actualResult.GetResultData();
            //Assert Meta data
            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i].ToString(), actualResData[i]);
            }
        }
    }
}