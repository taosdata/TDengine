using TDengineDriver;
using System.Runtime.InteropServices;

namespace TDengineExample
{
    internal class QueryExample
    {
        static void Main() { 
            IntPtr conn = GetConnection();
            // run query
            IntPtr res = TDengine.Query(conn, "SELECT * FROM meters LIMIT 2");
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.WriteLine("Failed to query since: " + TDengine.Error(res));  
                TDengine.Close(conn);
                TDengine.Cleanup();
                return;
            }

            // get filed count
            int fieldCount = TDengine.FieldCount(res);
            Console.WriteLine("fieldCount=" + fieldCount); 

            // print column names
            List<TDengineMeta> metas = TDengine.FetchFields(res);
            for (int i = 0; i < metas.Count; i++)
            {
                Console.Write(metas[i].name + "\t");
            }
            Console.WriteLine();

            // print values
            IntPtr row;
            while ((row = TDengine.FetchRows(res)) != IntPtr.Zero) {
                for (int i = 0; i < fieldCount; i++) { 
                    var meta = metas[i];
                    int offset = IntPtr.Size * i;
                    IntPtr data = Marshal.ReadIntPtr(row, offset);
                    if (data == IntPtr.Zero)
                    {
                        Console.Write("NULL\t");
                        continue;
                    }
                    switch ((TDengineDataType)meta.type)
                    {
                        case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                            bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                            Console.Write(v1 + "\t"); 
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                            byte v2 = Marshal.ReadByte(data);
                            Console.Write(v2 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                            short v3 = Marshal.ReadInt16(data);
                            Console.Write(v3 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_INT:
                            int v4 = Marshal.ReadInt32(data);
                            Console.Write(v4 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                            long v5 = Marshal.ReadInt64(data);
                            Console.Write(v5 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                            float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                            Console.Write(v6 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                            double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                            Console.WriteLine(v7 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                            string v8 = Marshal.PtrToStringAnsi(data);
                            Console.Write(v8 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            long v9 = Marshal.ReadInt64(data);
                            Console.Write(v9 + "\t");
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                            string v10 = Marshal.PtrToStringAnsi(data);
                            Console.Write(v10 + "\t");
                            break;
                    }
                }
                Console.WriteLine();
            }
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.WriteLine($"Query is not complete, Error { TDengine.ErrorNo(res)} {TDengine.Error(res)}");
            }
            // exit
            TDengine.FreeResult(res);
            TDengine.Close(conn);
            TDengine.Cleanup();
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
                Console.WriteLine("Connect to TDengine failed");
                System.Environment.Exit(0);
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
// 1648432611249   10.3    219     0.31    Beijing.Chaoyang        2
// 1648432611749   12.6    218     0.33    Beijing.Chaoyang        2