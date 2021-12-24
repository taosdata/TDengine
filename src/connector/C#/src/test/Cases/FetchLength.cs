using System;
using Test.UtilsTools;
using System.Collections.Generic;

namespace Cases
{

    public class FetchLengthCase
    {
        /// <author>xiaolei</author>
        /// <Name>TestRetrieveBinary</Name>
        /// <describe>TD-12103 C# connector fetch_row with binary data retrieving error</describe>
        /// <filename>FetchLength.cs</filename>
        /// <result>pass or failed </result>   
        public void TestRetrieveBinary(IntPtr conn)
        {
            string sql1 = "create stable stb1 (ts timestamp, name binary(10)) tags(n int);";
            string sql2 = "insert into tb1 using stb1 tags(1) values(now, 'log');";
            string sql3 = "insert into tb2 using stb1 tags(2) values(now, 'test');";
            string sql4 = "insert into tb3 using stb1 tags(3) values(now, 'db02');";
            string sql5 = "insert into tb4 using stb1 tags(4) values(now, 'db3');";

            string sql6 = "select distinct(name) from stb1;";//

            UtilsTools.ExecuteQuery(conn, sql1);
            UtilsTools.ExecuteQuery(conn, sql2);
            UtilsTools.ExecuteQuery(conn, sql3);
            UtilsTools.ExecuteQuery(conn, sql4);
            UtilsTools.ExecuteQuery(conn, sql5);

            IntPtr resPtr = IntPtr.Zero;
            resPtr = UtilsTools.ExecuteQuery(conn, sql6);
            List<List<string>> result = UtilsTools.GetResultSet(resPtr);

            List<string> colname = result[0];
            List<string> data = result[1];
            UtilsTools.AssertEqual("db3", data[0]);
            UtilsTools.AssertEqual("log", data[1]);
            UtilsTools.AssertEqual("db02", data[2]);
            UtilsTools.AssertEqual("test", data[3]);

        }
    }
}
