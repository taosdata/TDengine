using System;
using System.Collections.Generic;
using TDengineDriver;
using TDengineDriver.Impl;
using System.Runtime.InteropServices;

namespace TDengineExample
{
    public class AsyncQueryExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                QueryAsyncCallback queryAsyncCallback = new QueryAsyncCallback(QueryCallback);
                TDengine.QueryAsync(conn, "select * from meters", queryAsyncCallback, IntPtr.Zero);
                Thread.Sleep(2000);
            }
            finally
            {
                TDengine.Close(conn);
            }

        }

        static void QueryCallback(IntPtr param, IntPtr taosRes, int code)
        {
            if (code == 0 && taosRes != IntPtr.Zero)
            {
                FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                TDengine.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
            }
            else
            {
                throw new Exception($"async query data failed,code:{code},reason:{TDengine.Error(taosRes)}");
            }
        }

        // Iteratively call this interface until "numOfRows" is no greater than 0.
        static void FetchRawBlockCallback(IntPtr param, IntPtr taosRes, int numOfRows)
        {
            if (numOfRows > 0)
            {
                Console.WriteLine($"{numOfRows} rows async retrieved");
                IntPtr pdata = TDengine.GetRawBlock(taosRes);
                List<TDengineMeta> metaList = TDengine.FetchFields(taosRes);
                List<object> dataList = LibTaos.ReadRawBlock(pdata, metaList, numOfRows);

                for (int i = 0; i < dataList.Count; i++)
                {
                    if (i != 0 && (i + 1) % metaList.Count == 0)
                    {
                        Console.WriteLine("{0}\t|", dataList[i]);
                    }
                    else
                    {
                        Console.Write("{0}\t|", dataList[i]);
                    }
                }
                Console.WriteLine("");
                TDengine.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
            }
            else
            {
                if (numOfRows == 0)
                {
                    Console.WriteLine("async retrieve complete.");
                }
                else
                {
                    throw new Exception($"FetchRawBlockCallback callback error, error code {numOfRows}");
                }
                TDengine.FreeResult(taosRes);
            }
        }

        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "power";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }
    }
}

// //output:
// // Connect to TDengine success
// // 8 rows async retrieved

// // 1538548685500   |       11.8    |       221     |       0.28    |       california.losangeles   |       2       |
// // 1538548696600   |       13.4    |       223     |       0.29    |       california.losangeles   |       2       |
// // 1538548685000   |       10.8    |       223     |       0.29    |       california.losangeles   |       3       |
// // 1538548686500   |       11.5    |       221     |       0.35    |       california.losangeles   |       3       |
// // 1538548685000   |       10.3    |       219     |       0.31    |       california.sanfrancisco         |       2       |
// // 1538548695000   |       12.6    |       218     |       0.33    |       california.sanfrancisco         |       2       |
// // 1538548696800   |       12.3    |       221     |       0.31    |       california.sanfrancisco         |       2       |
// // 1538548696650   |       10.3    |       218     |       0.25    |       california.sanfrancisco         |       3       |
// // async retrieve complete.