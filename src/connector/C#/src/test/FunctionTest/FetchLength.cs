using System;
using Test.UtilsTools;
using System.Collections.Generic;
using Xunit;
using TDengineDriver;
using Test.UtilsTools.ResultSet;
namespace Cases
{
    public class FetchLengthCase
    {
        /// <author>xiaolei</author>
        /// <Name>TestRetrieveBinary</Name>
        /// <describe>TD-12103 C# connector fetch_row with binary data retrieving error</describe>
        /// <filename>FetchLength.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "Skip FetchLengthCase.TestRetrieveBinary()")]
        public void TestRetrieveBinary()
        {
            IntPtr conn = UtilsTools.TDConnection();
            var expectData = new List<string> { "log", "test", "db02", "db3" };
            var expectMeta = new List<TDengineMeta>{
                             UtilsTools.ConstructTDengineMeta("ts","timestamp"),
                             UtilsTools.ConstructTDengineMeta("name","binary(10)"),
                             UtilsTools.ConstructTDengineMeta("n","int")
                             };
            string sql0 = "drop table if exists stb1;";
            string sql1 = "create stable if not exists stb1 (ts timestamp, name binary(10)) tags(n int);";
            string sql2 = $"insert into tb1 using stb1 tags(1) values(now, '{expectData[0]}');";
            string sql3 = $"insert into tb2 using stb1 tags(2) values(now, '{expectData[1]}');";
            string sql4 = $"insert into tb3 using stb1 tags(3) values(now, '{expectData[2]}');";
            string sql5 = $"insert into tb4 using stb1 tags(4) values(now, '{expectData[3]}');";

            string sql6 = "select distinct(name) from stb1;";
            UtilsTools.ExecuteQuery(conn, sql0);
            UtilsTools.ExecuteQuery(conn, sql1);
            UtilsTools.ExecuteQuery(conn, sql2);
            UtilsTools.ExecuteQuery(conn, sql3);
            UtilsTools.ExecuteQuery(conn, sql4);
            UtilsTools.ExecuteQuery(conn, sql5);

            IntPtr resPtr = IntPtr.Zero;
            resPtr = UtilsTools.ExecuteQuery(conn, sql6);

            ResultSet actualResult = new ResultSet(resPtr);
            List<string> actualData = actualResult.GetResultData();
            List<TDengineMeta> actualMeta = actualResult.GetResultMeta();
            expectData.Reverse();

            Assert.Equal(expectData[0], actualData[0]);
            Assert.Equal(expectMeta[1].name, actualMeta[0].name);
            Assert.Equal(expectMeta[1].size, actualMeta[0].size);
            Assert.Equal(expectMeta[1].type, actualMeta[0].type);

        }
    }
}
