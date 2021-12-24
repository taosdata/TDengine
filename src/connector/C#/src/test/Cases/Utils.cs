using System;
using TDengineDriver;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;
namespace Test.UtilsTools
{
    public class UtilsTools
    {

        static string configDir = "C:/TDengine/cfg";

        public static IntPtr TDConnection(string ip, string user, string password, string db, short port)
        {
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_CONFIGDIR, configDir);
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            TDengine.Init();
            return TDengine.Connect(ip, user, password, db, port);
        }

        public static IntPtr ExecuteQuery(IntPtr conn, String sql)
        {
            IntPtr res = TDengine.Query(conn, sql);
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                Console.Write(sql.ToString() + " failure, ");
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));

                }
                Console.WriteLine("");
                ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");
            }
            return res;
        }

        public static IntPtr ExecuteErrorQuery(IntPtr conn, String sql)
        {
            IntPtr res = TDengine.Query(conn, sql);
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                Console.Write(sql.ToString() + " failure, ");
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));

                }
                Console.WriteLine("");
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");

            }
            return res;
        }

        public static void DisplayRes(IntPtr res)
        {
            long queryRows = 0;
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));
                }
                Console.WriteLine("");
                ExitProgram();
            }

            int fieldCount = TDengine.FieldCount(res);
            List<TDengineMeta> metas = TDengine.FetchFields(res);
            for (int j = 0; j < metas.Count; j++)
            {
                TDengineMeta meta = (TDengineMeta)metas[j];
            }

            IntPtr rowdata;
            StringBuilder builder = new StringBuilder();

            while ((rowdata = TDengine.FetchRows(res)) != IntPtr.Zero)
            {
                queryRows++;
                IntPtr colLengthPtr = TDengine.FetchLengths(res);
                int[] colLengthArr = new int[fieldCount];
                Marshal.Copy(colLengthPtr, colLengthArr, 0, fieldCount);
                for (int fields = 0; fields < fieldCount; ++fields)
                {
                    TDengineMeta meta = metas[fields];
                    int offset = IntPtr.Size * fields;
                    IntPtr data = Marshal.ReadIntPtr(rowdata, offset);

                    builder.Append("---");

                    if (data == IntPtr.Zero)
                    {
                        builder.Append("NULL");
                        continue;
                    }

                    switch ((TDengineDataType)meta.type)
                    {
                        case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                            bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                            builder.Append(v1);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                            byte v2 = Marshal.ReadByte(data);
                            builder.Append(v2);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                            short v3 = Marshal.ReadInt16(data);
                            builder.Append(v3);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_INT:
                            int v4 = Marshal.ReadInt32(data);
                            builder.Append(v4);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                            long v5 = Marshal.ReadInt64(data);
                            builder.Append(v5);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                            float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                            builder.Append(v6);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                            double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                            builder.Append(v7);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                            string v8 = Marshal.PtrToStringAnsi(data, colLengthArr[fields]);
                            builder.Append(v8);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            long v9 = Marshal.ReadInt64(data);
                            builder.Append(v9);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                            string v10 = Marshal.PtrToStringAnsi(data, colLengthArr[fields]);
                            builder.Append(v10);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                            string v11 = Marshal.PtrToStringAnsi(data);
                            builder.Append(v11);
                            break;
                    }
                }
                builder.Append("---");

                if (queryRows <= 10)
                {
                    Console.WriteLine(builder.ToString());
                }
                builder.Clear();
            }

            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0:G}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
            TDengine.FreeResult(res); Console.WriteLine("");
        }

        public static List<List<string>> GetResultSet(IntPtr res)
        {
            List<List<string>> result = new List<List<string>>();
            List<string> colName = new List<string>();
            List<string> dataRaw = new List<string>();
            long queryRows = 0;
            if (!IsValidResult(res))
            {
                ExitProgram();
            }

            int fieldCount = TDengine.FieldCount(res);
            List<TDengineMeta> metas = TDengine.FetchFields(res);

            for (int j = 0; j < metas.Count; j++)
            {
                TDengineMeta meta = (TDengineMeta)metas[j];
                colName.Add(meta.name);
            }
            result.Add(colName);

            IntPtr rowdata;
            while ((rowdata = TDengine.FetchRows(res)) != IntPtr.Zero)
            {
                queryRows++;
                IntPtr colLengthPtr = TDengine.FetchLengths(res);
                int[] colLengthArr = new int[fieldCount];
                Marshal.Copy(colLengthPtr, colLengthArr, 0, fieldCount);

                for (int fields = 0; fields < fieldCount; ++fields)
                {
                    TDengineMeta meta = metas[fields];
                    int offset = IntPtr.Size * fields;
                    IntPtr data = Marshal.ReadIntPtr(rowdata, offset);

                    if (data == IntPtr.Zero)
                    {
                        dataRaw.Add("NULL");
                        continue;
                    }

                    switch ((TDengineDataType)meta.type)
                    {
                        case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                            bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                            dataRaw.Add(v1.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                            byte v2 = Marshal.ReadByte(data);
                            dataRaw.Add(v2.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                            short v3 = Marshal.ReadInt16(data);
                            dataRaw.Add(v3.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_INT:
                            int v4 = Marshal.ReadInt32(data);
                            dataRaw.Add(v4.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                            long v5 = Marshal.ReadInt64(data);
                            dataRaw.Add(v5.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                            float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                            dataRaw.Add(v6.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                            double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                            dataRaw.Add(v7.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                            string v8 = Marshal.PtrToStringAnsi(data, colLengthArr[fields]);
                            dataRaw.Add(v8);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            long v9 = Marshal.ReadInt64(data);
                            dataRaw.Add(v9.ToString());
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                            string v10 = Marshal.PtrToStringAnsi(data, colLengthArr[fields]);
                            dataRaw.Add(v10);
                            break;
                    }
                }

            }
            result.Add(dataRaw);

            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0:G}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
            TDengine.FreeResult(res); Console.WriteLine("");
            return result;
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
        public static void CloseConnection(IntPtr conn)
        {
            if (conn != IntPtr.Zero)
            {
                if (TDengine.Close(conn) == 0)
                {
                    Console.WriteLine("close connection sucess");
                }
                else
                {
                    Console.WriteLine("close Connection failed");
                }
            }
        }
        public static List<TDengineMeta> getField(IntPtr res)
        {
            List<TDengineMeta> metas = TDengine.FetchFields(res);
            return metas;
        }
        public static void AssertEqual(string expectVal, string actualVal)
        {
            if (expectVal == actualVal)
            {
                Console.WriteLine("{0}=={1} pass", expectVal, actualVal);
            }
            else
            {
                Console.WriteLine("{0}=={1} failed", expectVal, actualVal);
                ExitProgram();
            }
        }
        public static void ExitProgram()
        {
            TDengine.Cleanup();
            System.Environment.Exit(0);
        }
    }
}
