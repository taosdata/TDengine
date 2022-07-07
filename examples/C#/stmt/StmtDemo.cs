/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Collections;
namespace TDengineDriver
{
    public class StmtDemo
    {
        //connection parameters
        private string host = "127.0.0.1";
        private string configDir = "C:/TDengine/cfg";
        private string user = "root";
        private string passwd = "taosdata";
        private short port = 0;

        private IntPtr conn = IntPtr.Zero;
        private IntPtr stmt = IntPtr.Zero;

        static void Main(string[] args)
        {
            string dropDB = "drop database if exists csharp";
            string createDB = "create database if not exists csharp keep 36500";
            string selectDB = "use csharp";
            string stmtSql = "insert into ? using stmtdemo tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            string createTable = "create stable stmtdemo (ts timestamp "
                                + ",b bool"
                                + ",v1 tinyint"
                                + ",v2 smallint"
                                + ",v4 int"
                                + ",v8 bigint"
                                + ",f4 float"
                                + ",f8 double"
                                + ",u1 tinyint unsigned"
                                + ",u2 smallint unsigned"
                                + ",u4 int unsigned"
                                + ",u8 bigint unsigned"
                                + ",bin binary(200)"
                                + ",blob nchar(200)"
                                + ")tags("
                                + "bo bool"
                                + ",tt tinyint"
                                + ",si smallint"
                                + ",ii int"
                                + ",bi bigint"
                                + ",tu tinyint unsigned"
                                + ",su smallint unsigned"
                                + ",iu int unsigned"
                                + ",bu bigint unsigned"
                                + ",ff float "
                                + ",dd double "
                                + ",bb binary(200)"
                                + ",nc nchar(200)"
                                + ")";

            string dropTable = "drop table if exists stmtdemo";

            string tableName = "t1";
            StmtDemo stmtDemo = new StmtDemo();
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("Start StmtDemo insert  Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            stmtDemo.InitTDengine();
            //TDengine connect
            stmtDemo.ConnectTDengine();

            //before stmt 
            stmtDemo.ExecuteQuery(dropDB);
            stmtDemo.ExecuteQuery(createDB);
            stmtDemo.ExecuteQuery(selectDB);
            stmtDemo.ExecuteQuery(dropTable);
            stmtDemo.ExecuteQuery(createTable);

            stmtDemo.StmtInit();
            // string[] tableList = { "stmtdemo" };
            // stmtDemo.loadTableInfo(tableList);

            stmtDemo.StmtPrepare(stmtSql);
            TAOS_BIND[] binds = stmtDemo.InitBindArr();
            TAOS_MULTI_BIND[] mbinds = stmtDemo.InitMultBindArr();
            stmtDemo.SetTableNameTags(tableName, binds);
            stmtDemo.BindParamBatch(mbinds);
            stmtDemo.AddBatch();
            stmtDemo.StmtExecute();
            TaosBind.FreeTaosBind(binds);
            TaosMultiBind.FreeTaosBind(mbinds);
            stmtDemo.StmtClose();

            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("start StmtDemo select Testing...");
            Console.WriteLine("---------------------------------------------------------------");

            stmtDemo.StmtInit();
            string selectSql = "SELECT * FROM stmtdemo WHERE v1 > ? AND v4 < ?";

            stmtDemo.StmtPrepare(selectSql);

            TAOS_BIND[] queryCondition = new TAOS_BIND[2];
            queryCondition[0] = TaosBind.BindTinyInt(0);
            queryCondition[1] = TaosBind.BindInt(1000);

            Console.WriteLine(selectSql);
            stmtDemo.BindParam(queryCondition);
            stmtDemo.StmtExecute();

            stmtDemo.StmtUseResult();

            stmtDemo.StmtClose();
            TaosBind.FreeTaosBind(queryCondition);
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("Stop StmtDemo  Testing...");
            Console.WriteLine("---------------------------------------------------------------");

            stmtDemo.CloseConnection();
        }
        public TAOS_BIND[] InitBindArr()
        {
            TAOS_BIND[] binds = new TAOS_BIND[13];
            binds[0] = TaosBind.BindBool(true);
            binds[1] = TaosBind.BindTinyInt(-2);
            binds[2] = TaosBind.BindSmallInt(short.MaxValue);
            binds[3] = TaosBind.BindInt(int.MaxValue);
            binds[4] = TaosBind.BindBigInt(Int64.MaxValue);
            binds[5] = TaosBind.BindUTinyInt(byte.MaxValue - 1);
            binds[6] = TaosBind.BindUSmallInt(UInt16.MaxValue - 1);
            binds[7] = TaosBind.BindUInt(uint.MinValue + 1);
            binds[8] = TaosBind.BindUBigInt(UInt64.MinValue + 1);
            binds[9] = TaosBind.BindFloat(11.11F);
            binds[10] = TaosBind.BindDouble(22.22D);
            binds[11] = TaosBind.BindBinary("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            binds[12] = TaosBind.BindNchar("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            return binds;
        }

        public TAOS_MULTI_BIND[] InitMultBindArr()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[14];
            long[] tsArr = new long[5] { 1637064040000, 1637064041000, 1637064042000, 1637064043000, 1637064044000 };
            bool?[] boolArr = new bool?[5] { true, false, null, true, true };
            sbyte?[] tinyIntArr = new sbyte?[5] { -127, 0, null, 8, 127 };
            short?[] shortArr = new short?[5] { short.MinValue + 1, -200, null, 100, short.MaxValue };
            int?[] intArr = new int?[5] { -200, -100, null, 0, 300 };
            long?[] longArr = new long?[5] { long.MinValue + 1, -2000, null, 1000, long.MaxValue };
            float?[] floatArr = new float?[5] { float.MinValue + 1, -12.1F, null, 0F, float.MaxValue };
            double?[] doubleArr = new double?[5] { double.MinValue + 1, -19.112D, null, 0D, double.MaxValue };
            byte?[] uTinyIntArr = new byte?[5] { byte.MinValue, 12, null, 89, byte.MaxValue - 1 };
            ushort?[] uShortArr = new ushort?[5] { ushort.MinValue, 200, null, 400, ushort.MaxValue - 1 };
            uint?[] uIntArr = new uint?[5] { uint.MinValue, 100, null, 2, uint.MaxValue - 1 };
            ulong?[] uLongArr = new ulong?[5] { ulong.MinValue, 2000, null, 1000, long.MaxValue - 1 };
            string[] binaryArr = new string[5] { "1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", String.Empty, null, "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM", "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890~!@#$%^&*()_+=-`[]{}:,./<>?" };
            string[] ncharArr = new string[5] { "1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", null, "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM", "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", string.Empty };
            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindBool(boolArr);
            mBinds[2] = TaosMultiBind.MultiBindTinyInt(tinyIntArr);
            mBinds[3] = TaosMultiBind.MultiBindSmallInt(shortArr);
            mBinds[4] = TaosMultiBind.MultiBindInt(intArr);
            mBinds[5] = TaosMultiBind.MultiBindBigint(longArr);
            mBinds[6] = TaosMultiBind.MultiBindFloat(floatArr);
            mBinds[7] = TaosMultiBind.MultiBindDouble(doubleArr);
            mBinds[8] = TaosMultiBind.MultiBindUTinyInt(uTinyIntArr);
            mBinds[9] = TaosMultiBind.MultiBindUSmallInt(uShortArr);
            mBinds[10] = TaosMultiBind.MultiBindUInt(uIntArr);
            mBinds[11] = TaosMultiBind.MultiBindUBigInt(uLongArr);
            mBinds[12] = TaosMultiBind.MultiBindBinary(binaryArr);
            mBinds[13] = TaosMultiBind.MultiBindNchar(ncharArr);
            return mBinds;
        }

        public void loadTableInfo(string[] arr)
        {
            if (TDengine.LoadTableInfo(this.conn, arr) == 0)
            {
                Console.WriteLine("load table info success");
            }
            else
            {
                Console.WriteLine("load table info failed");
                ExitProgram();
            }
        }

        public void InitTDengine()
        {
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_CONFIGDIR, this.configDir);
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            TDengine.Init();
            Console.WriteLine("TDengine Initialization finished");
        }

        public void ConnectTDengine()
        {
            string db = "";
            this.conn = TDengine.Connect(this.host, this.user, this.passwd, db, this.port);
            if (this.conn == IntPtr.Zero)
            {
                Console.WriteLine("Connect to TDengine failed");
                ExitProgram();
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
        }

        public void ExecuteQuery(String sql)
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
            TDengine.FreeResult(res);
        }

        public void StmtInit()
        {
            this.stmt = TDengine.StmtInit(conn);
            if (this.stmt == IntPtr.Zero)
            {
                Console.WriteLine("Init stmt failed");
                ExitProgram();
            }
            else
            {
                Console.WriteLine("Init stmt success");
            }
        }

        public void StmtPrepare(string sql)
        {
            int res = TDengine.StmtPrepare(this.stmt, sql);
            if (res == 0)
            {
                Console.WriteLine("stmt prepare success");
            }
            else
            {
                Console.WriteLine("stmt prepare failed " + TDengine.StmtErrorStr(stmt));
                ExitProgram();
            }
        }

        public void SetTableName(String tableName)
        {
            int res = TDengine.StmtSetTbname(this.stmt, tableName);
            Console.WriteLine("setTableName():" + res);
            if (res == 0)
            {
                Console.WriteLine("set_tbname success");
            }
            else
            {
                Console.Write("set_tbname failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }

        public void SetTableNameTags(String tableName, TAOS_BIND[] tags)
        {
            int res = TDengine.StmtSetTbnameTags(this.stmt, tableName, tags);
            if (res == 0)
            {
                Console.WriteLine("set tbname && tags success");

            }
            else
            {
                Console.Write("set tbname && tags failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }

        public void SetSubTableName(string name)
        {
            int res = TDengine.StmtSetSubTbname(this.stmt, name);
            if (res == 0)
            {
                Console.WriteLine("set subtable name success");
            }
            else
            {
                Console.Write("set subtable name failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }

        }

        public void BindParam(TAOS_BIND[] binds)
        {
            Console.WriteLine("in bindParam()");

            int res = TDengine.StmtBindParam(this.stmt, binds);
            if (res == 0)
            {
                Console.WriteLine("bind  para success");
            }
            else
            {
                Console.Write("bind  para failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }

        public void BindSingleParamBatch(TAOS_MULTI_BIND bind, int index)
        {
            int res = TDengine.StmtBindSingleParamBatch(this.stmt,ref bind, index);
            if (res == 0)
            {
                Console.WriteLine("single bind  batch success");
            }
            else
            {
                Console.Write("single bind  batch failed: " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }

        public void BindParamBatch(TAOS_MULTI_BIND[] bind)
        {
            int res = TDengine.StmtBindParamBatch(this.stmt, bind);
            if (res == 0)
            {
                Console.WriteLine("bind  parameter batch success");
            }
            else
            {
                Console.WriteLine("bind  parameter batch failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }

        public void AddBatch()
        {
            int res = TDengine.StmtAddBatch(this.stmt);
            if (res == 0)
            {
                Console.WriteLine("stmt add batch success");
            }
            else
            {
                Console.Write("stmt add batch failed,reason: " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }
        public void StmtExecute()
        {
            int res = TDengine.StmtExecute(this.stmt);
            if (res == 0)
            {
                Console.WriteLine("Execute stmt success");
            }
            else
            {
                Console.Write("Execute stmt failed,reason: " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }
        public void StmtClose()
        {
            int res = TDengine.StmtClose(this.stmt);
            if (res == 0)
            {
                Console.WriteLine("close stmt success");
            }
            else
            {
                Console.WriteLine("close stmt failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose();
                ExitProgram();
            }
        }
        public void CloseConnection()
        {
            if (this.conn != IntPtr.Zero)
            {
                if (TDengine.Close(this.conn) == 0)
                {
                    Console.WriteLine("close connection sucess");
                }
                else
                {
                    Console.WriteLine("close Connection failed");
                }
            }
        }

        //select only
        public void StmtUseResult()
        {
            IntPtr res = TDengine.StmtUseResult(this.stmt);
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));
                }
                Console.WriteLine("");
                StmtClose();
                CloseConnection();
                ExitProgram();
            }
            else
            {
                Console.WriteLine("{0},query success");
                DisplayRes(res);
                TDengine.FreeResult(res);
            }

        }

        public void DisplayRes(IntPtr res)
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
                            string v8 = Marshal.PtrToStringAnsi(data);
                            builder.Append(v8);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            long v9 = Marshal.ReadInt64(data);
                            builder.Append(v9);
                            break;
                        case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                            string v10 = Marshal.PtrToStringAnsi(data);
                            builder.Append(v10);
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
            Console.WriteLine("");

        }
        public static void ExitProgram()
        {
            TDengine.Cleanup();
            System.Environment.Exit(1);
        }
    }
}
