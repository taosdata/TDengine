using System;
using TDengineDriver;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;
namespace Test.UtilsTools
{
    public class UtilsTools
    {

        static string ip = "127.0.0.1";
        static string user = "root";
        static string password = "taosdata";
        static string db = "";
        static short port = 0;
        //get a tdengine connection
        public static IntPtr TDConnection()
        {
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_CONFIGDIR, GetConfigPath());
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            TDengine.Init();
            IntPtr conn = TDengine.Connect(ip, user, password, db, port);
            return conn;
        }
        //get taos.cfg file based on different os
        public static string GetConfigPath()
        {
              string configDir = "" ;
        if(OperatingSystem.IsOSPlatform("Windows"))
        {   
            configDir = "C:/TDengine/cfg";
        }
        else if(OperatingSystem.IsOSPlatform("Linux"))
        {
            configDir = "/etc/taos";
        }
        else if(OperatingSystem.IsOSPlatform("macOS"))
        {
            configDir = "/etc/taos";
        }
        return configDir;
        }

        public static IntPtr ExecuteQuery(IntPtr conn, String sql)
        {
            IntPtr res = TDengine.Query(conn, sql);
            if (!IsValidResult(res))
            {
                Console.Write(sql.ToString() + " failure, ");
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
            if (!IsValidResult(res))
            {
                Console.Write(sql.ToString() + " failure, ");
                ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");

            }
            return res;
        }

        public static void ExecuteUpdate(IntPtr conn, String sql)
        {
            IntPtr res = TDengine.Query(conn, sql);
            if (!IsValidResult(res))
            {
                Console.Write(sql.ToString() + " failure, ");
                ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");

            }
            TDengine.FreeResult(res);
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
            TDengine.Cleanup();
        }
        public static List<TDengineMeta> GetResField(IntPtr res)
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
            System.Environment.Exit(1);
        }
        public static List<Object> GetResData(IntPtr res)
        {
            List<Object> dataRaw = new List<Object>();
            if (!IsValidResult(res))
            {
                ExitProgram();
            }
            List<TDengineMeta> metas = GetResField(res);
            dataRaw = QueryRes(res, metas);
            return dataRaw;
        }

        public static TDengineMeta ConstructTDengineMeta(string name, string type)
        {

            TDengineMeta _meta = new TDengineMeta();
            _meta.name = name;
            char[] separators = new char[] { '(', ')' };
            string[] subs = type.Split(separators, StringSplitOptions.RemoveEmptyEntries);

            switch (subs[0].ToUpper())
            {
                case "BOOL":
                    _meta.type = 1;
                    _meta.size = 1;
                    break;
                case "TINYINT":
                    _meta.type = 2;
                    _meta.size = 1;
                    break;
                case "SMALLINT":
                    _meta.type = 3;
                    _meta.size = 2;
                    break;
                case "INT":
                    _meta.type = 4;
                    _meta.size = 4;
                    break;
                case "BIGINT":
                    _meta.type = 5;
                    _meta.size = 8;
                    break;
                case "TINYINT UNSIGNED":
                    _meta.type = 11;
                    _meta.size = 1;
                    break;
                case "SMALLINT UNSIGNED":
                    _meta.type = 12;
                    _meta.size = 2;
                    break;
                case "INT UNSIGNED":
                    _meta.type = 13;
                    _meta.size = 4;
                    break;
                case "BIGINT UNSIGNED":
                    _meta.type = 14;
                    _meta.size = 8;
                    break;
                case "FLOAT":
                    _meta.type = 6;
                    _meta.size = 4;
                    break;
                case "DOUBLE":
                    _meta.type = 7;
                    _meta.size = 8;
                    break;
                case "BINARY":
                    _meta.type = 8;
                    _meta.size = short.Parse(subs[1]);
                    break;
                case "TIMESTAMP":
                    _meta.type = 9;
                    _meta.size = 8;
                    break;
                case "NCHAR":
                    _meta.type = 10;
                    _meta.size = short.Parse(subs[1]);
                    break;
                case "JSON":
                    _meta.type = 15;
                    _meta.size = 4096;
                    break;
                default:
                    _meta.type = byte.MaxValue;
                    _meta.size = 0;
                    break;
            }
            return _meta;
        }

        private static List<Object> QueryRes(IntPtr res, List<TDengineMeta> metas)
        {
            IntPtr rowdata;
            long queryRows = 0;
            List<Object> dataRaw = new List<Object>();
            int fieldCount = metas.Count;
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
                            // string v8 = Marshal.PtrToStringAnsi(data, colLengthArr[fields]);
                            string v8 = Marshal.PtrToStringUTF8(data, colLengthArr[fields]);
                            dataRaw.Add(v8);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            long v9 = Marshal.ReadInt64(data);
                            dataRaw.Add(v9);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                            // string v10 = Marshal.PtrToStringAnsi(data, colLengthArr[fields]);
                            string v10 = Marshal.PtrToStringUTF8(data, colLengthArr[fields]);
                            dataRaw.Add(v10);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                            byte v12 = Marshal.ReadByte(data);
                            dataRaw.Add(v12);
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
                        default:
                            dataRaw.Add("unknown value");
                            break;
                    }
                }

            }
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0:G}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
            TDengine.FreeResult(res);
            Console.WriteLine("");
            return dataRaw;
        }

        // Generate insert sql for the with the coldata and tag data 
        public static string ConstructInsertSql(string table,string stable,List<Object> colData,List<Object> tagData,int numOfRows)
        {
            int numofFileds = colData.Count / numOfRows;
            StringBuilder insertSql;

            if (stable == "")
            {
                insertSql = new StringBuilder($"insert into {table} values(");
            }
            else
            {
                insertSql = new StringBuilder($"insert into {table} using {stable} tags(");

                for (int j = 0; j < tagData.Count; j++)
                {
                    if (tagData[j] is String)
                    {
                        insertSql.Append('\'');
                        insertSql.Append(tagData[j]);
                        insertSql.Append('\'');
                    }
                    else
                    {
                        insertSql.Append(tagData[j]);
                    }
                    if (j + 1 != tagData.Count)
                    {
                        insertSql.Append(',');
                    }
                }

                insertSql.Append(")values(");
            }
            for (int i = 0; i < colData.Count; i++)
            {

                if (colData[i] is String)
                {
                    insertSql.Append('\'');
                    insertSql.Append(colData[i]);
                    insertSql.Append('\'');
                }
                else
                {
                    insertSql.Append(colData[i]);
                }

                if ((i + 1) % numofFileds == 0 && (i + 1) != colData.Count)
                {
                    insertSql.Append(")(");
                }
                else if ((i + 1) == colData.Count)
                {
                    insertSql.Append(')');
                }
                else
                {
                    insertSql.Append(',');
                }
            }
            insertSql.Append(';');
            //Console.WriteLine(insertSql.ToString());

            return insertSql.ToString();
        }
    
        public static List<object> CombineColAndTagData(List<object> colData,List<object> tagData, int numOfRows)
        {
            var list = new List<Object>();
            for (int i = 0; i < colData.Count; i++)
            {
                list.Add(colData[i]);
                if ((i + 1) % (colData.Count / numOfRows) == 0)
                {
                    for (int j = 0; j < tagData.Count; j++)
                    {
                        list.Add(tagData[j]);
                    }
                }
            }
            return list;
        }
    }
}
