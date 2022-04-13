using System;
using TDengineDriver;
using System.Runtime.InteropServices;
using System.Text;
using System.Collections.Generic;
namespace Sample.UtilsTools
{
    public class UtilsTools
    {

        static string ip = "127.0.0.1";
        static string user = "root";
        static string password = "taosdata";
        static string db = "";
        static short port = 0;
        static string globalDbName = "csharp_example_db";
        //get a TDengine connection
        public static IntPtr TDConnection(string dbName = "csharp_example_db")
        {
            TDengine.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR, GetConfigPath());
            TDengine.Options((int)TDengineInitOption.TSDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            TDengine.Init();

            IntPtr conn = TDengine.Connect(ip, user, password, db, port);

            UtilsTools.ExecuteUpdate(conn, $"drop database if  exists {dbName}");
            UtilsTools.ExecuteUpdate(conn, $"create database if not exists {dbName} keep 3650");
            UtilsTools.ExecuteUpdate(conn, $"use {dbName}");

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

            List<TDengineMeta> metaList = GetResField(res);
            int fieldCount = metaList.Count;
            // metaList.ForEach((item) => { Console.Write("{0} ({1}) \t|\t", item.name, item.size); });

            List<Object> dataList = QueryRes(res, metaList);
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

        public static List<List<Object>> GetResultSet(IntPtr res)
        {
            List<List<Object>> result = new List<List<Object>>();
            List<Object> colName = new List<Object>();
            List<Object> dataRaw = new List<Object>();
            if (!IsValidResult(res))
            {
                ExitProgram();
            }

            List<TDengineMeta> meta = GetResField(res);
            result.Add(colName);

            dataRaw = QueryRes(res, meta);
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
            ExecuteUpdate(conn, $"drop database if  exists {globalDbName}");
            if (conn != IntPtr.Zero)
            {
                TDengine.Close(conn);
                Console.WriteLine("close connection success");
            }
            else
            {
                throw new Exception("connection if already null");
            }
        }
        public static List<TDengineMeta> GetResField(IntPtr res)
        {
            List<TDengineMeta> meta = TDengine.FetchFields(res);
            return meta;
        }
        public static void ExitProgram()
        {
            TDengine.Cleanup();
            System.Environment.Exit(0);
        }
        public static List<Object> GetResData(IntPtr res)
        {
            List<Object> colName = new List<Object>();
            List<Object> dataRaw = new List<Object>();
            if (!IsValidResult(res))
            {
                ExitProgram();
            }
            List<TDengineMeta> meta = GetResField(res);
            dataRaw = QueryRes(res, meta);
            return dataRaw;
        }

        private static List<Object> QueryRes(IntPtr res, List<TDengineMeta> meta)
        {
            IntPtr taosRow;
            List<Object> dataRaw = new List<Object>();
            int fieldCount = meta.Count;
            while ((taosRow = TDengine.FetchRows(res)) != IntPtr.Zero)
            {
                dataRaw.AddRange(FetchRow(taosRow, res));
            }
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0:G}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
            // TDengine.FreeResult(res);
            Console.WriteLine("");
            return dataRaw;
        }


        public static List<Object> FetchRow(IntPtr taosRow, IntPtr taosRes)//, List<TDengineMeta> metaList, int numOfFiled
        {
            List<TDengineMeta> metaList = TDengine.FetchFields(taosRes);
            int numOfFiled = TDengine.FieldCount(taosRes);

            List<Object> dataRaw = new List<Object>();

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
    }
}

