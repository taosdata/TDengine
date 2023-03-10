using TDengineDriver;
using TDengineDriver.Impl;
using System.Runtime.InteropServices;

namespace TDengineExample
{
    internal class QueryExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                // run query
                IntPtr res = TDengine.Query(conn, "SELECT * FROM meters LIMIT 2");
                if (TDengine.ErrorNo(res) != 0)
                {
                    throw new Exception("Failed to query since: " + TDengine.Error(res));
                }

                // get filed count
                int fieldCount = TDengine.FieldCount(res);
                Console.WriteLine("fieldCount=" + fieldCount);

                // print column names
                List<TDengineMeta> metas = LibTaos.GetMeta(res);
                for (int i = 0; i < metas.Count; i++)
                {
                    Console.Write(metas[i].name + "\t");
                }
                Console.WriteLine();

                // print values
                List<Object> resData = LibTaos.GetData(res);
                for (int i = 0; i < resData.Count; i++)
                {
                    Console.Write($"|{resData[i].ToString()} \t");
                    if (((i + 1) % metas.Count == 0))
                    {
                        Console.WriteLine("");
                    }
                }
                Console.WriteLine();

                // Free result after use
                TDengine.FreeResult(res);
            }
            finally
            {
                TDengine.Close(conn);
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

// output:
// Connect to TDengine success
// fieldCount=6
// ts      current voltage phase   location        groupid
// 1648432611249   10.3    219     0.31    California.SanFrancisco        2
// 1648432611749   12.6    218     0.33    California.SanFrancisco        2