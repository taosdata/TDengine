using System;
using System.Collections.Generic;
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
    public class FetchFieldCases
    {
        readonly DatabaseFixture _database;
        private readonly ITestOutputHelper _output;

        public FetchFieldCases(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this._database = fixture;
            this._output = output;
        }

        /// <author>xiaolei</author>
        /// <Name>FetchFieldsCases.TestFetchFieldsJsonTag</Name>
        /// <describe>test taos_fetch_fields(), check the meta data</describe>
        /// <filename>FetchFields.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "FetchFieldsCases.TestFetchFieldJsonTag()"), TestExeOrder(1),
         Trait("Category", "FetchFieldJsonTag")]
        public void TestFetchFieldJsonTag()
        {
            IntPtr conn = _database.Conn;
            Assert.NotEqual(conn, IntPtr.Zero);
            IntPtr _res = IntPtr.Zero;
            string tableName = "fetch_fields";
            var expectResMeta = new List<TDengineMeta>
            {
                Tools.ConstructTDengineMeta("ts", "timestamp"),
                Tools.ConstructTDengineMeta("b", "bool"),
                Tools.ConstructTDengineMeta("v1", "tinyint"),
                Tools.ConstructTDengineMeta("v2", "smallint"),
                Tools.ConstructTDengineMeta("v4", "int"),
                Tools.ConstructTDengineMeta("v8", "bigint"),
                Tools.ConstructTDengineMeta("f4", "float"),
                Tools.ConstructTDengineMeta("f8", "double"),
                Tools.ConstructTDengineMeta("u1", "tinyint unsigned"),
                Tools.ConstructTDengineMeta("u2", "smallint unsigned"),
                Tools.ConstructTDengineMeta("u4", "int unsigned"),
                Tools.ConstructTDengineMeta("u8", "bigint unsigned"),
                Tools.ConstructTDengineMeta("bin", "binary(200)"),
                Tools.ConstructTDengineMeta("c_nchar", "nchar(200)"),
                Tools.ConstructTDengineMeta("jsontag", "json"),
            };
            var expectResData = new List<String>
            {
                "1637064040000", "true", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "XI", "XII",
                "{\"k1\": \"v1\"}"
            };
            String dropTb = "drop table if exists " + tableName;
            string createTb = $"create stable {tableName}" +
                              "(ts timestamp" +
                              ",b bool" +
                              ",v1 tinyint" +
                              ",v2 smallint" +
                              ",v4 int" +
                              ",v8 bigint" +
                              ",f4 float" +
                              ",f8 double" +
                              ",u1 tinyint unsigned" +
                              ",u2 smallint unsigned" +
                              ",u4 int unsigned" +
                              ",u8 bigint unsigned" +
                              ",bin binary(200)" +
                              ",c_nchar nchar(200)" +
                              ")" +
                              "tags" +
                              "(jsontag json);";

            String insertSql = $"insert into {tableName}_t1 using {tableName} " +
                               " tags('{\"k1\": \"v1\"}') " +
                               "values(1637064040000,true,1,2,3,4,5,6,7,8,9,10,'XI','XII')";
            String selectSql = $"select * from {tableName}";
            String dropSql = Tools.DropTable(String.Empty, tableName);

            Tools.ExecuteUpdate(conn, dropTb, _output);
            Tools.ExecuteUpdate(conn, createTb, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);
            _res = Tools.ExecuteQuery(conn, selectSql, _output);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.ResultMeta;

            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }

            _output.WriteLine("FetchFieldsCases.TestFetchFieldJsonTag() pass");
        }
    }
}