using System.Text;
using Xunit.Abstractions;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengine.Driver;
using TDengine.Driver.Impl;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Test.Utils
{
    public class Tools
    {
        /*----------------------- construct SQL ----------------------*/
        public static string CreateDB(string db)
        {
            return $"create database if not exists {db} keep 36500 WAL_RETENTION_PERIOD 86400";
        }

        public static string UseDB(string db)
        {
            return $"use {db}";
        }

        public static string CreateTable(string table, bool ifStable = true, bool ifJson = false)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.Append($"create table if not exists {table}");
            sqlBuilder.Append("(");
            sqlBuilder.Append("ts timestamp");
            sqlBuilder.Append(",v1 tinyint");
            sqlBuilder.Append(",v2 smallint");
            sqlBuilder.Append(",v4 int");
            sqlBuilder.Append(",v8 bigint");
            sqlBuilder.Append(",u1 tinyint unsigned");
            sqlBuilder.Append(",u2 smallint unsigned");
            sqlBuilder.Append(",u4 int unsigned");
            sqlBuilder.Append(",u8 bigint unsigned");
            sqlBuilder.Append(",f4 float");
            sqlBuilder.Append(",f8 double");
            sqlBuilder.Append(",bin binary(200)");
            sqlBuilder.Append(",nchr nchar(200)");
            sqlBuilder.Append(",b bool");
            sqlBuilder.Append(")");
            if (ifStable)
            {
                sqlBuilder.Append("tags(");
                if (ifJson)
                {
                    sqlBuilder.Append("json_tag json");
                }
                else
                {
                    sqlBuilder.Append("bo bool");
                    sqlBuilder.Append(",tt tinyint");
                    sqlBuilder.Append(",si smallint");
                    sqlBuilder.Append(",ii int");
                    sqlBuilder.Append(",bi bigint");
                    sqlBuilder.Append(",tu tinyint unsigned");
                    sqlBuilder.Append(",su smallint unsigned");
                    sqlBuilder.Append(",iu int unsigned");
                    sqlBuilder.Append(",bu bigint unsigned");
                    sqlBuilder.Append(",ff float");
                    sqlBuilder.Append(",dd double");
                    sqlBuilder.Append(",bb binary(200)");
                    sqlBuilder.Append(",nc nchar(200)");
                }

                sqlBuilder.Append(')');
            }

            return sqlBuilder.ToString();
        }

        public static string DropDB(string db)
        {
            return $"drop database if exists {db}";
        }

        public static string DropTable(string db, string table)
        {
            if (string.IsNullOrEmpty(db))
            {
                return $"drop table if exists {table}";
            }
            else
            {
                return $"drop table if exists {db}.{table}";
            }
        }

        // Generate insert SQL for the with the columns' data and tags' data 
        public static string ConstructInsertSql(string table, string stable, List<Object> colData,
            List<Object> tagData, int numOfRows)
        {
            int numOfFields = colData.Count / numOfRows;
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
                    switch (tagData[j])
                    {
                        case string val:
                            insertSql.Append('\'');
                            insertSql.Append(val);
                            insertSql.Append('\'');
                            break;
                        case byte[] val:
                            insertSql.Append('\'');
                            insertSql.Append(Encoding.UTF8.GetString(val));
                            insertSql.Append('\'');
                            break;
                        case DateTime val:
                            var ts = TDengineConstant.ConvertDateTimeToTimestamp(val,
                                TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                            insertSql.Append(ts);
                            break;
                        default:
                            insertSql.Append(tagData[j]);
                            break;
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
                switch (colData[i])
                {
                    case string val:
                        insertSql.Append('\'');
                        insertSql.Append(val);
                        insertSql.Append('\'');
                        break;
                    case byte[] val:
                        insertSql.Append('\'');
                        insertSql.Append(Encoding.UTF8.GetString(val));
                        insertSql.Append('\'');
                        break;
                    case DateTime val:
                        var ts = TDengineConstant.ConvertDateTimeToTimestamp(val,
                            TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                        insertSql.Append(ts);
                        break;
                    default:
                        insertSql.Append(colData[i]);
                        break;
                }

                if ((i + 1) % numOfFields == 0 && (i + 1) != colData.Count)
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

        /*---------------------- warp native methods --------------*/
        public static IntPtr ExecuteQuery(IntPtr conn, String sql, ITestOutputHelper output)
        {
            IntPtr res = NativeMethods.Query(conn, sql);
            if (!IsValidResult(res))
            {
                throw new Exception($"execute {sql} failed.");
            }

            return res;
        }

        public static IntPtr ExecuteQueryWithReqId(IntPtr conn, String sql, ITestOutputHelper output, long reqid)
        {
            IntPtr res = NativeMethods.QueryWithReqid(conn, sql, reqid);
            if (!IsValidResult(res))
            {
                throw new Exception($"execute {sql} failed.");
            }

            return res;
        }

        public static IntPtr ExecuteErrorQuery(IntPtr conn, String sql)
        {
            IntPtr res = NativeMethods.Query(conn, sql);
            if (!IsValidResult(res))
            {
                throw new Exception($"execute {sql} failed.");
            }

            return res;
        }

        public static void ExecuteUpdate(IntPtr conn, String sql, ITestOutputHelper output)
        {
            IntPtr res = NativeMethods.Query(conn, sql);
            if (!IsValidResult(res))
            {
                throw new Exception($"execute {sql} failed.");
            }

            NativeMethods.FreeResult(res);
        }

        public static void FreeResult(IntPtr res)
        {
            NativeMethods.FreeResult(res);
        }

        /*----------------------- For Test ------------*/
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
                    _meta.size = int.Parse(subs[1]);
                    break;
                case "TIMESTAMP":
                    _meta.type = 9;
                    _meta.size = 8;
                    break;
                case "NCHAR":
                    _meta.type = 10;
                    _meta.size = int.Parse(subs[1]);
                    break;
                case "JSON":
                    _meta.type = 15;
                    _meta.size = 4095;
                    break;
                default:
                    _meta.type = byte.MaxValue;
                    _meta.size = 0;
                    break;
            }

            return _meta;
        }

        public static List<TDengineMeta> GetMetaFromDDL(string dllStr)
        {
            var expectResMeta = new List<TDengineMeta>();
            //"CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);";
            int bracketInd = dllStr.IndexOf("(");
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
            string subDllStr = dllStr.Substring(bracketInd);

            String[] stableSeparators = new String[] { "tags", "TAGS" };
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)
            //(location BINARY(30), groupId INT)
            String[] dllStrElements = subDllStr.Split(stableSeparators, StringSplitOptions.RemoveEmptyEntries);
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)
            dllStrElements[0] = dllStrElements[0].Substring(1, dllStrElements[0].Length - 2);
            string[] finalStr1 = dllStrElements[0].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (string item in finalStr1)
            {
                //ts TIMESTAMP
                string[] itemArr = item.Split(new[] { ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);
                // Console.WriteLine("GetMetaFromDLL():{0},{1}",itemArr[0],itemArr[1]);
                expectResMeta.Add(Tools.ConstructTDengineMeta(itemArr[0], itemArr[1]));
            }

            if (dllStr.Contains("TAGS") || dllStr.Contains("tags"))
            {
                //location BINARY(30), groupId INT
                dllStrElements[1] = dllStrElements[1].Substring(1, dllStrElements[1].Length - 2);
                //location BINARY(30)  groupId INT
                String[] finalStr2 = dllStrElements[1].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string item in finalStr2)
                {
                    //location BINARY(30)
                    string[] itemArr = item.Split(new[] { ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);
                    // Console.WriteLine("GetMetaFromDLL():{0},{1}",itemArr[0],itemArr[1]);
                    expectResMeta.Add(Tools.ConstructTDengineMeta(itemArr[0], itemArr[1]));
                }
            }

            return expectResMeta;
        }

        public static List<object> ConstructResData(List<object> colData, List<object> tagData, int numOfRows)
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

        public static List<Object> ColumnsList(int numOfRows)
        {
            var now = DateTime.Now;
            var nowTs = TDengineConstant.ConvertDateTimeToTimestamp(now, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            List<object> columns = new List<object>();
            for (int i = 0; i < numOfRows; i++)
            {
                var ts = TDengineConstant.ConvertTimestampToDateTime(nowTs + i * 10,
                    TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
                columns.Add(ts);
                columns.Add((sbyte)(-10 + i));
                columns.Add((short)(-20 + i));
                columns.Add(-30 + i);
                columns.Add((long)(-40 + i));
                columns.Add((byte)i);
                columns.Add((ushort)(i + 1));
                columns.Add((uint)(i + 2));
                columns.Add((ulong)(i + 3));
                columns.Add((float)(3.1415F + i));
                columns.Add((double)(3.14159265358979D + i));
                columns.Add(Encoding.UTF8.GetBytes("binary_col_列_" + i));
                columns.Add("nchar_col_列_" + i);
                columns.Add((i & 1) == 1 ? true : false);
            }

            return columns;
        }

        public static List<Object> TagsList(int seq, bool ifJson = false)
        {
            List<object> tags = new List<object>();
            List<Object> jTags = new List<Object>();
            if (seq > 3 || seq < 1)
            {
                throw new IndexOutOfRangeException("seq should in range 1-3");
            }

            if (ifJson)
            {
                switch (seq)
                {
                    case 1:
                        jTags.Add(Encoding.UTF8.GetBytes(
                            "{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":1,\"key5\":true}"));
                        break;
                    case 2:
                        jTags.Add(Encoding.UTF8.GetBytes(
                            "{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":2,\"key5\":false}"));
                        break;
                    case 3:
                        jTags.Add(Encoding.UTF8.GetBytes(
                            "{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":3,\"key5\":true}"));
                        break;
                    default:
                        throw new IndexOutOfRangeException("seq should in range 1-3");
                }

                return jTags;
            }
            else
            {
                tags.Add((seq & 1) == 1 ? true : false);
                tags.Add((sbyte)(-10 + seq));
                tags.Add((short)(-20 + seq));
                tags.Add(-30 + seq);
                tags.Add((long)(-40 + seq));
                tags.Add((byte)(0 + seq));
                tags.Add((ushort)(1 + seq));
                tags.Add((uint)(2 + seq));
                tags.Add((ulong)(3 + seq));
                tags.Add((float)(3.1415F + seq));
                tags.Add((double)(3.14159265358979D + seq));
                tags.Add(Encoding.UTF8.GetBytes("binary_tag_标签_" + seq));
                tags.Add("nchar_tag_标签_" + seq);

                return tags;
            }
        }

        /*-------------------- other utility functions -----------------*/
        public static bool IsValidResult(IntPtr res)
        {
            if ((res == IntPtr.Zero))
            {
                throw new Exception(
                    $"invalid TAOS_RES,reason {NativeMethods.Error(res)} code:{NativeMethods.ErrorNo(res)}");
            }

            int code = NativeMethods.ErrorNo(res);
            if (code != 0)
            {
                throw new Exception(
                    $"invalid TAOS_RES,reason {NativeMethods.Error(res)} code:{NativeMethods.ErrorNo(res)}");
            }

            return true;
        }

        public static void ExitProgram(int statusCode = 1)
        {
            NativeMethods.Cleanup();
            System.Environment.Exit(statusCode);
        }

        public static List<object> GetData(IntPtr taosRes)
        {
            List<TDengineMeta> metaList = NativeMethods.FetchFields(taosRes);
            List<Object> list = new List<object>();

            IntPtr numOfRowsPrt = Marshal.AllocHGlobal(sizeof(Int32));
            IntPtr pDataPtr = Marshal.AllocHGlobal(IntPtr.Size);
            IntPtr pData;
            try
            {
                byte[] colType = new byte[metaList.Count];
                for (int i = 0; i < metaList.Count; i++)
                {
                    colType[i] = metaList[i].type;
                }

                while (true)
                {
                    int code = NativeMethods.FetchRawBlock(taosRes, numOfRowsPrt, pDataPtr);
                    if (code != 0)
                    {
                        throw new Exception(
                            $"fetch_raw_block failed,code {code} reason:{NativeMethods.Error(taosRes)}");
                    }

                    int numOfRows = Marshal.ReadInt32(numOfRowsPrt);
                    if (numOfRows == 0)
                    {
                        break;
                    }

                    //list = new List<Object>(numOfRows * numOfFields);
                    pData = Marshal.ReadIntPtr(pDataPtr);
                    list.AddRange(ReadRawBlock(pData, metaList, numOfRows));
                }

                return list;
            }
            finally
            {
                Marshal.FreeHGlobal(numOfRowsPrt);
                Marshal.FreeHGlobal(pDataPtr);
            }
        }

        public static List<object> ReadRawBlock(IntPtr pData, List<TDengineMeta> metaList, int numOfRows)
        {
            var list = new List<object>(metaList.Count * numOfRows);
            byte[] colType = new byte[metaList.Count];
            byte[] scales = new byte[metaList.Count];
            for (int i = 0; i < metaList.Count; i++)
            {
                colType[i] = metaList[i].type;
                scales[i] = metaList[i].scale;
            }

            var br = new BlockReader(0, metaList.Count, (int)TDenginePrecision.TSDB_TIME_PRECISION_MILLI, colType,
                scales);
            br.SetBlockPtr(pData, numOfRows);
            for (int rowIndex = 0; rowIndex < numOfRows; rowIndex++)
            {
                for (int colIndex = 0; colIndex < metaList.Count; colIndex++)
                {
                    list.Add(br.Read(rowIndex, colIndex));
                }
            }

            return list;
        }
    }
}