using System;
using TDengineDriver;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;
using Xunit.Abstractions;
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
            // UtilsTools.ExecuteUpdate(conn, "drop database if  exists csharp");
            UtilsTools.ExecuteUpdate(conn, "create database if not exists csharp keep 3650");
            UtilsTools.ExecuteUpdate(conn, "use csharp");
            return conn;
        }
        //get taos.cfg file based on different os
        public static string GetConfigPath()
        {
            string configDir = "";
            if (OperatingSystem.IsOSPlatform("Windows"))
            {
                configDir = "C:/TDengine/cfg";
            }
            else if (OperatingSystem.IsOSPlatform("Linux"))
            {
                configDir = "/etc/taos";
            }
            else if (OperatingSystem.IsOSPlatform("macOS"))
            {
                configDir = "/usr/local/etc/taos";
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

        public static void DisplayRes(IntPtr res)
        {
            if (!IsValidResult(res))
            {
                ExitProgram();
            }

            List<TDengineMeta> metas = GetResField(res);
            int fieldCount = metas.Count;

            IntPtr rowdata;
            List<string> datas = QueryRes(res, metas);
            for (int i = 0; i < metas.Count; i++)
            {
                for (int j = 0; j < datas.Count; j++)
                {
                    Console.Write(" {0} \t|", datas[j]);
                }
                Console.WriteLine("");
            }

        }

        public static List<List<string>> GetResultSet(IntPtr res)
        {
            List<List<string>> result = new List<List<string>>();
            List<string> colName = new List<string>();
            List<string> dataRaw = new List<string>();
            if (!IsValidResult(res))
            {
                ExitProgram();
            }

            List<TDengineMeta> metas = GetResField(res);
            result.Add(colName);

            dataRaw = QueryRes(res, metas);
            result.Add(dataRaw);

            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0:G}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
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
            ExecuteUpdate(conn, "drop database if  exists csharp");
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
            System.Environment.Exit(0);
        }
        public static List<String> GetResData(IntPtr res)
        {
            List<string> dataRaw = GetResDataWithoutFree(res);
            FreeResult(res);
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

        private static List<string> QueryRes(IntPtr res, List<TDengineMeta> metas)
        {
            IntPtr taosRow;
            List<string> dataRaw = new List<string>();
            int fieldCount = metas.Count;
            while ((taosRow = TDengine.FetchRows(res)) != IntPtr.Zero)
            {
                dataRaw.AddRange(FetchRow(taosRow, res));
            }
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0:G}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
            Console.WriteLine("");
            return dataRaw;
        }

        // Generate insert sql for the with the coldata and tag data 
        public static string ConstructInsertSql(string table, string stable, List<Object> colData, List<Object> tagData, int numOfRows)
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

        public static List<object> CombineColAndTagData(List<object> colData, List<object> tagData, int numOfRows)
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

        /// <summary>
        /// Using this method to free TAOS_RES,otherwise will lead memory
        /// leak.Notice do not call this method while subscribe/consume until
        /// end of the program.
        /// </summary>
        /// <param name="res">TAOS_RES, the resultset usually is return by taos_query()</param>
        public static void FreeResult(IntPtr res)
        {
            TDengine.FreeResult(res);
        }


        /// <summary>
        /// Using to parse TAOS_ROW.
        /// </summary>
        /// <param name="taosRow">This is TAOS_RES pointer</param>
        /// <param name="taosRes"> This is TAOS_ROW pointer</param>
        /// <returns></returns>
        public static List<string> FetchRow(IntPtr taosRow, IntPtr taosRes)
        {
            List<TDengineMeta> metaList = TDengine.FetchFields(taosRes);
            int numOfFiled = TDengine.FieldCount(taosRes);

            List<String> dataRaw = new List<string>();

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
                        bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                        dataRaw.Add(v1.ToString());
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                        sbyte v2 = (sbyte)Marshal.ReadByte(data);
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
                        string v8 = Marshal.PtrToStringUTF8(data, colLengthArr[i]);
                        dataRaw.Add(v8);
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                        long v9 = Marshal.ReadInt64(data);
                        dataRaw.Add(v9.ToString());
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
                        dataRaw.Add(v13.ToString());
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_UINT:
                        uint v14 = (uint)Marshal.ReadInt32(data);
                        dataRaw.Add(v14.ToString());
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                        ulong v15 = (ulong)Marshal.ReadInt64(data);
                        dataRaw.Add(v15.ToString());
                        break;
                    case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                        string v16 = Marshal.PtrToStringUTF8(data, colLengthArr[i]);
                        dataRaw.Add(v16);
                        break;
                    default:
                        dataRaw.Add("nonsupport data type value");
                        break;
                }

            }
            return dataRaw;
        }

        /// <summary>
        /// Get the result data from TAO_RES but this interface will
        /// not free the TAO_RES at the end. Remember to free the TAOS_RES
        /// when you need to do so.
        /// </summary>
        /// <param name="res"> This is a TAOS_RES pointer.</param>
        /// <returns></returns>
        public static List<String> GetResDataWithoutFree(IntPtr res)
        {
            List<string> colName = new List<string>();
            List<string> dataRaw = new List<string>();
            if (!IsValidResult(res))
            {
                ExitProgram();
            }
            List<TDengineMeta> metas = GetResField(res);
            dataRaw = QueryRes(res, metas);
            return dataRaw;
        }
    }

}

