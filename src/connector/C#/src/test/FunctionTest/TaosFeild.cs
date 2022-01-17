using System;
using Test.UtilsTools;
using TDengineDriver;
using System.Collections.Generic;
using Xunit;
using Test.UtilsTools.ResultSet;
namespace Cases
{
    public class FetchFieldCases
    {
        /// <author>xiaolei</author>
        /// <Name>FetchFieldCases.TestFetchFieldJsonTag</Name>
        /// <describe>test taos_fetch_fields(), check the meta data</describe>
        /// <filename>TaosFeild.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "FetchFieldCases.TestFetchFieldJsonTag()")]
        public void TestFetchFieldJsonTag()
        {
            IntPtr conn = UtilsTools.TDConnection();
            IntPtr _res = IntPtr.Zero;
            string tableName = "fetchfeilds";
            var expectResMeta = new List<TDengineMeta> {
                UtilsTools.ConstructTDengineMeta("ts", "timestamp"),
                UtilsTools.ConstructTDengineMeta("b", "bool"),
                UtilsTools.ConstructTDengineMeta("v1", "tinyint"),
                UtilsTools.ConstructTDengineMeta("v2", "smallint"),
                UtilsTools.ConstructTDengineMeta("v4", "int"),
                UtilsTools.ConstructTDengineMeta("v8", "bigint"),
                UtilsTools.ConstructTDengineMeta("f4", "float"),
                UtilsTools.ConstructTDengineMeta("f8", "double"),
                UtilsTools.ConstructTDengineMeta("u1", "tinyint unsigned"),
                UtilsTools.ConstructTDengineMeta("u2", "smallint unsigned"),
                UtilsTools.ConstructTDengineMeta("u4", "int unsigned"),
                UtilsTools.ConstructTDengineMeta("u8", "bigint unsigned"),
                UtilsTools.ConstructTDengineMeta("bin", "binary(200)"),
                UtilsTools.ConstructTDengineMeta("blob", "nchar(200)"),
                UtilsTools.ConstructTDengineMeta("jsontag", "json"),
            };
            var expectResData = new List<String> { "1637064040000", "true", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "XI", "XII", "{\"k1\": \"v1\"}" };
            String dropTb = "drop table if exists " + tableName;
            String createTb = "create stable " + tableName
                                + " (ts timestamp" +
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
                                ",blob nchar(200)" +
                                ")" +
                                "tags" +
                                "(jsontag json);";
            String insertSql = "insert into " + tableName + "_t1 using " + tableName +
                               " tags('{\"k1\": \"v1\"}') " +
                               "values(1637064040000,true,1,2,3,4,5,6,7,8,9,10,'XI','XII')";
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;

            UtilsTools.ExecuteUpdate(conn, dropTb);
            UtilsTools.ExecuteUpdate(conn, createTb);
            UtilsTools.ExecuteUpdate(conn, insertSql);
            _res = UtilsTools.ExecuteQuery(conn, selectSql);

            ResultSet actualResult = new ResultSet(_res);
            List<TDengineMeta> actualMeta = actualResult.GetResultMeta();
            for (int i = 0; i < actualMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
            }
        }
    }
}
