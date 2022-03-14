using System;
using TDengineDriver;
using Sample.UtilsTools;
using System.Runtime.InteropServices;
using System.Threading;

namespace Example
{
    public class AsyncQuerySample
    {
        public void RunQueryAsync(IntPtr conn, string table)
        {
            QueryAsyncCallback queryAsyncCallback = new QueryAsyncCallback(QueryCallback);
            PrepareData(conn, table);
            Console.WriteLine($"Start calling QueryAsync(),query {table}'s data asynchronously.");
            TDengine.QueryAsync(conn, $"select * from {table}", queryAsyncCallback, IntPtr.Zero);
            Thread.Sleep(2000);
            Console.WriteLine("QueryAsync done.");

        }

        //prepare the data(table and insert data)
        public void PrepareData(IntPtr conn, string tableName)
        {
            string createTable = $"create table if not exists {tableName} (ts timestamp,i8 tinyint,i16 smallint,i32 int,i64 bigint)tags(t_bnry binary(50), t_nchr nchar(50));";
            string insert1 = $"insert into {tableName}_s01 using {tableName} tags('tag1','标签1') values(now,1,2,3,4)(now+1m,5,6,7,8)(now+2m,9,0,-1,-2)(now+3m,-3,-4,-5,-6)(now+4m,-7,-8,-9,0)";
            string insert2 = $"insert into {tableName}_s02 using {tableName} tags('tag2','标签2') values(now,1,2,3,4)(now+1m,5,6,7,8)(now+2m,9,0,-1,-2)(now+3m,-3,-4,-5,-6)(now+4m,-7,-8,-9,0)";
            string insert3 = $"insert into {tableName}_s03 using {tableName} tags('tag3','标签3') values(now,1,2,3,4)(now+1m,5,6,7,8)(now+2m,9,0,-1,-2)(now+3m,-3,-4,-5,-6)(now+4m,-7,-8,-9,0)";
            string insert4 = $"insert into {tableName}_s04 using {tableName} tags('tag4','标签4') values(now,1,2,3,4)(now+1m,5,6,7,8)(now+2m,9,0,-1,-2)(now+3m,-3,-4,-5,-6)(now+4m,-7,-8,-9,0)";
            string insert5 = $"insert into {tableName}_s05 using {tableName} tags('tag5','标签5') values(now,1,2,3,4)(now+1m,5,6,7,8)(now+2m,9,0,-1,-2)(now+3m,-3,-4,-5,-6)(now+4m,-7,-8,-9,0)";

            UtilsTools.ExecuteUpdate(conn, createTable);
            UtilsTools.ExecuteUpdate(conn, insert1);
            Thread.Sleep(100);
            UtilsTools.ExecuteUpdate(conn, insert2);
            Thread.Sleep(100);
            UtilsTools.ExecuteUpdate(conn, insert3);
            Thread.Sleep(100);
            UtilsTools.ExecuteUpdate(conn, insert4);
            Thread.Sleep(100);
            UtilsTools.ExecuteUpdate(conn, insert5);

        }

        public void QueryCallback(IntPtr param, IntPtr taosRes, int code)
        {
            if (code == 0 && taosRes != IntPtr.Zero)
            {
                FetchRowAsyncCallback fetchRowAsyncCallback = new FetchRowAsyncCallback(FetchCallback);
                TDengine.FetchRowAsync(taosRes, fetchRowAsyncCallback, param);
            }
            else
            {
                Console.WriteLine($"async query data failed, failed code {code}");
            }
        }

        // Iteratively call this interface until "numOfRows" is no greater than 0.
        public void FetchCallback(IntPtr param, IntPtr taosRes, int numOfRows)
        {
            if (numOfRows > 0)
            {
                Console.WriteLine($"{numOfRows} rows async retrieved");
                UtilsTools.DisplayRes(taosRes);
                TDengine.FetchRowAsync(taosRes, FetchCallback, param);
            }
            else
            {
                if (numOfRows == 0)
                {
                    Console.WriteLine("async retrieve complete.");

                }
                else
                {
                    Console.WriteLine($"FetchRowAsync callback error, error code {numOfRows}");
                }
                TDengine.FreeResult(taosRes);
            }
        }
    }


}