using System;
using Test.Utils;
using System.Collections.Generic;
using System.Text;
using TDengine.Driver;
using Test.Utils.ResultSet;
using Test.Case.Attributes;
using Xunit;
using Xunit.Abstractions;
using Test.Fixture;


namespace Function.Test.Taosc
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]

    public class FetchLengthCase
    {

        DatabaseFixture _database;
        private readonly ITestOutputHelper _output;

        public FetchLengthCase(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this._database = fixture;
            this._output = output;

        }

        /// <author>xiaolei</author>
        /// <Name>TestRetrieveBinary</Name>
        /// <describe>TD-12103 C# connector fetch_row with binary data retrieving error</describe>
        /// <filename>FetchLength.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "FetchLengthCase.TestRetrieveBinary()"), TestExeOrder(1)]
        public void TestRetrieveBinary()
        {
            IntPtr conn = _database.Conn;
            var expectData = new List<string> { "log", "test", "db02", "db3" };
            var expectMeta = new List<TDengineMeta>{
                             Tools.ConstructTDengineMeta("ts","timestamp"),
                             Tools.ConstructTDengineMeta("name","binary(10)"),
                             Tools.ConstructTDengineMeta("n","int")
                             };
            string sql0 = "drop table if exists stb1;";
            string sql1 = "create stable if not exists stb1 (ts timestamp, name binary(10)) tags(n int);";
            string sql2 = $"insert into tb1 using stb1 tags(1) values(now, '{expectData[0]}');";
            string sql3 = $"insert into tb2 using stb1 tags(2) values(now+1s, '{expectData[1]}');";
            string sql4 = $"insert into tb3 using stb1 tags(3) values(now+2s, '{expectData[2]}');";
            string sql5 = $"insert into tb4 using stb1 tags(4) values(now+3s, '{expectData[3]}');";

            string sql6 = "select name from stb1 order by ts asc ;";
            Tools.ExecuteQuery(conn, sql0, _output);
            Tools.ExecuteQuery(conn, sql1, _output);
            Tools.ExecuteQuery(conn, sql2, _output);
            Tools.ExecuteQuery(conn, sql3, _output);
            Tools.ExecuteQuery(conn, sql4, _output);
            Tools.ExecuteQuery(conn, sql5, _output);

            IntPtr resPtr = IntPtr.Zero;
            resPtr = Tools.ExecuteQuery(conn, sql6, _output);

            ResultSet actualResult = new ResultSet(resPtr);
            List<Object> actualData = actualResult.ResultData;
            List<TDengineMeta> actualMeta = actualResult.ResultMeta;

            // Assert meta data
            Assert.Equal(expectMeta[1].name, actualMeta[0].name);
            Assert.Equal(expectMeta[1].size, actualMeta[0].size);
            Assert.Equal(expectMeta[1].type, actualMeta[0].type);

            // Assert retrieved data
            for (int i = 0; i < actualData.Count; i++)
            {
                // Console.WriteLine($"expectData[{i}]:{expectData[i]},, actualData[{i}]:{actualData[i]}" );
                Assert.Equal(expectData[i], Encoding.UTF8.GetString((byte[])actualData[i]));
            }
            _output.WriteLine("FetchLengthCase.TestRetrieveBinary() pass");

        }
    }
}

