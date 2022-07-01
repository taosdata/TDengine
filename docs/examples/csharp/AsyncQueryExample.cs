using TDengineDriver;
using System.Runtime.InteropServices;

namespace TDengineExample
{
    public class AsyncQueryExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            QueryAsyncCallback queryAsyncCallback = new QueryAsyncCallback(QueryCallback);
            TDengine.QueryAsync(conn, "select * from meters", queryAsyncCallback, IntPtr.Zero);
            Thread.Sleep(2000);
            TDengine.Close(conn);
            TDengine.Cleanup();
        }

        static void QueryCallback(IntPtr param, IntPtr taosRes, int code)
        {
            if (code == 0 && taosRes != IntPtr.Zero)
            {
                FetchRowAsyncCallback fetchRowAsyncCallback = new FetchRowAsyncCallback(FetchRowCallback);
                TDengine.FetchRowAsync(taosRes, fetchRowAsyncCallback, param);
            }
            else
            {
                Console.WriteLine($"async query data failed, failed code {code}");
            }
        }

        static void FetchRowCallback(IntPtr param, IntPtr taosRes, int numOfRows)
        {
            if (numOfRows > 0)
            {
                Console.WriteLine($"{numOfRows} rows async retrieved");
                DisplayRes(taosRes);
                TDengine.FetchRowAsync(taosRes, FetchRowCallback, param);
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

        public static void DisplayRes(IntPtr res)
        {
            if (!IsValidResult(res))
            {
                TDengine.Cleanup();
                System.Environment.Exit(1);
            }

            List<TDengineMeta> metaList = TDengine.FetchFields(res);
            int fieldCount = metaList.Count;
            // metaList.ForEach((item) => { Console.Write("{0} ({1}) \t|\t", item.name, item.size); });

            List<object> dataList = QueryRes(res, metaList);
            for (int index = 0; index < dataList.Count; index++)
            {
                if (index % fieldCount == 0 && index != 0)
                {
                    Console.WriteLine("");
                }
                Console.Write("{0} \t|\t", dataList[index].ToString());

            }
            Console.WriteLine("");
        }

        public static bool IsValidResult(IntPtr res)
        {
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));
                    return false;
                }
                Console.WriteLine("");
                return false;
            }
            return true;
        }

        private static List<object> QueryRes(IntPtr res, List<TDengineMeta> meta)
        {
            IntPtr taosRow;
            List<object> dataRaw = new();
            while ((taosRow = TDengine.FetchRows(res)) != IntPtr.Zero)
            {
                dataRaw.AddRange(FetchRow(taosRow, res));
            }
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0} {1}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
            TDengine.FreeResult(res);
            Console.WriteLine("");
            return dataRaw;
        }

        public static List<object> FetchRow(IntPtr taosRow, IntPtr taosRes)//, List<TDengineMeta> metaList, int numOfFiled
        {
            List<TDengineMeta> metaList = TDengine.FetchFields(taosRes);
            int numOfFiled = TDengine.FieldCount(taosRes);


            List<object> dataRaw = new();

            IntPtr colLengthPrt = TDengine.FetchLengths(taosRes);
            int[] colLengthArr = new int[numOfFiled];
            Marshal.Copy(colLengthPrt, colLengthArr, 0, numOfFiled);

            for (int i = 0; i < numOfFiled; i++)
            {
                TDengineMeta meta = metaList[i];
                IntPtr data = Marshal.ReadIntPtr(taosRow, IntPtr.Size * i);

                if (data == IntPtr.Zero)
                {
                    dataRaw.Add("NULL");
                    continue;
                }
                switch ((TDengineDataType)meta.type)
                {
                    case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                        bool v1 = Marshal.ReadByte(data) != 0;
                        dataRaw.Add(v1);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                        sbyte v2 = (sbyte)Marshal.ReadByte(data);
                        dataRaw.Add(v2);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                        short v3 = Marshal.ReadInt16(data);
                        dataRaw.Add(v3);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_INT:
                        int v4 = Marshal.ReadInt32(data);
                        dataRaw.Add(v4);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                        long v5 = Marshal.ReadInt64(data);
                        dataRaw.Add(v5);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                        float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                        dataRaw.Add(v6);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                        double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                        dataRaw.Add(v7);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                        string v8 = Marshal.PtrToStringUTF8(data, colLengthArr[i]);
                        dataRaw.Add(v8);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                        long v9 = Marshal.ReadInt64(data);
                        dataRaw.Add(v9);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                        string v10 = Marshal.PtrToStringUTF8(data, colLengthArr[i]);
                        dataRaw.Add(v10);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                        byte v12 = Marshal.ReadByte(data);
                        dataRaw.Add(v12.ToString());
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                        ushort v13 = (ushort)Marshal.ReadInt16(data);
                        dataRaw.Add(v13);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_UINT:
                        uint v14 = (uint)Marshal.ReadInt32(data);
                        dataRaw.Add(v14);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                        ulong v15 = (ulong)Marshal.ReadInt64(data);
                        dataRaw.Add(v15);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                        string v16 = Marshal.PtrToStringUTF8(data, colLengthArr[i]);
                        dataRaw.Add(v16);
                        break;
                    default:
                        dataRaw.Add("nonsupport data type");
                        break;
                }

            }
            return dataRaw;
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
                Environment.Exit(0);
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }
    }
}

//output:
// Connect to TDengine success
// 8 rows async retrieved

// 1538548685500   |       11.8    |       221     |       0.28    |       california.losangeles   |       2       |
// 1538548696600   |       13.4    |       223     |       0.29    |       california.losangeles   |       2       |
// 1538548685000   |       10.8    |       223     |       0.29    |       california.losangeles   |       3       |
// 1538548686500   |       11.5    |       221     |       0.35    |       california.losangeles   |       3       |
// 1538548685000   |       10.3    |       219     |       0.31    |       california.sanfrancisco         |       2       |
// 1538548695000   |       12.6    |       218     |       0.33    |       california.sanfrancisco         |       2       |
// 1538548696800   |       12.3    |       221     |       0.31    |       california.sanfrancisco         |       2       |
// 1538548696650   |       10.3    |       218     |       0.25    |       california.sanfrancisco         |       3       |
// async retrieve complete.