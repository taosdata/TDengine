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
    public class stmtfunction
    {
        //connection parameters
        private string host = "127.0.0.1";
        private string configDir = "/etc/taos";
        private string user = "root";
        private string passwd = "taosdata";
        private short port = 6030;

        private IntPtr conn = IntPtr.Zero;
        private IntPtr stmt = IntPtr.Zero;

        //prepare the tags value
        //Integer
        public TAOS_BIND[] InitBindArr1()
        {
            TAOS_BIND[] binds = new TAOS_BIND[4];

            binds[0] = TaosBind.BindTinyInt(-2);
            binds[1] = TaosBind.BindSmallInt(short.MaxValue);
            binds[2] = TaosBind.BindInt(int.MaxValue);
            binds[3] = TaosBind.BindBigInt(Int64.MaxValue);

            return binds;
        }

        //unsigned Integer
        public TAOS_BIND[] InitBindArr2()
        {
            TAOS_BIND[] binds = new TAOS_BIND[4];

            binds[0] = TaosBind.BindUTinyInt(byte.MaxValue - 1);
            binds[1] = TaosBind.BindUSmallInt(UInt16.MaxValue - 1);
            binds[2] = TaosBind.BindUInt(uint.MinValue + 1);
            binds[3] = TaosBind.BindUBigInt(UInt64.MinValue + 1);

            return binds;
        }

        //float and double
        public TAOS_BIND[] InitBindArr3()
        {
            TAOS_BIND[] binds = new TAOS_BIND[6];

            binds[0] = TaosBind.BindFloat(11.11F);
            binds[1] = TaosBind.BindFloat(float.MinValue+1);
            binds[2] = TaosBind.BindFloat(float.MaxValue-1);
            binds[3] = TaosBind.BindDouble(22.22D);
            binds[4] = TaosBind.BindDouble(double.MinValue+1);
            binds[5] = TaosBind.BindDouble(double.MaxValue-1);


            return binds;
        }

        //binary and nchar
        public TAOS_BIND[] InitBindArr4()
        {
            TAOS_BIND[] binds = new TAOS_BIND[2];
            string a = "abcdABCD123`~!@#$%^&*()-=+_[]{}:;\",.<>/?\\\\'";
            string b = "abcdABCD123`~!@#$%^&*()-=+_[]{}:;\",.<>/?taos涛思";

            //Console.WriteLine(a);
            //Console.WriteLine(b);
            binds[0] = TaosBind.BindBinary(a);
            binds[1] = TaosBind.BindNchar(b);

            return binds;
        }

        //prepare the column values
        //Integer
        public TAOS_MULTI_BIND[] InitMultBindArr1()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[5];
            long[] tsArr = new long[5] { 1637064040000, 1637064041000, 1637064042000, 1637064043000, 1637064044000 };
            sbyte?[] tinyIntArr = new sbyte?[5] { -127, 0, null, 8, 127 };
            short?[] shortArr = new short?[5] { short.MinValue + 1, -200, null, 100, short.MaxValue };
            int?[] intArr = new int?[5] { -200, -100, null, 0, 300 };
            long?[] longArr = new long?[5] { long.MinValue + 1, -2000, null, 1000, long.MaxValue };

            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindTinyInt(tinyIntArr);
            mBinds[2] = TaosMultiBind.MultiBindSmallInt(shortArr);
            mBinds[3] = TaosMultiBind.MultiBindInt(intArr);
            mBinds[4] = TaosMultiBind.MultiBindBigint(longArr);

            return mBinds;
        }
        //Unsigned Integer
        public TAOS_MULTI_BIND[] InitMultBindArr2()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[5];
            long[] tsArr = new long[5] { 1637064040000, 1637064041000, 1637064042000, 1637064043000, 1637064044000 };
            byte?[] uTinyIntArr = new byte?[5] { byte.MinValue, 0, null, 89, byte.MaxValue - 1 };
            ushort?[] uShortArr = new ushort?[5] { ushort.MinValue, 0, null, 400, ushort.MaxValue - 1 };
            uint?[] uIntArr = new uint?[5] { uint.MinValue, 0, null, 2001, uint.MaxValue - 1 };
            ulong?[] uLongArr = new ulong?[5] { ulong.MinValue, 0, null, 1000, long.MaxValue - 1 };

            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindUTinyInt(uTinyIntArr);
            mBinds[2] = TaosMultiBind.MultiBindUSmallInt(uShortArr);
            mBinds[3] = TaosMultiBind.MultiBindUInt(uIntArr);
            mBinds[4] = TaosMultiBind.MultiBindUBigInt(uLongArr);

            return mBinds;
        }
        //float and double
        public TAOS_MULTI_BIND[] InitMultBindArr3()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[3];
            long[] tsArr = new long[5] { 1637064040000, 1637064041000, 1637064042000, 1637064043000, 1637064044000 };
            float?[] floatArr = new float?[5] { float.MinValue + 1, -12.1F, null, 0F, float.MaxValue };
            double?[] doubleArr = new double?[5] { double.MinValue + 1, -19.112D, null, 0D, double.MaxValue };

            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindFloat(floatArr);
            mBinds[2] = TaosMultiBind.MultiBindDouble(doubleArr);
            

            return mBinds;
        }
        //binary and nchar
        public TAOS_MULTI_BIND[] InitMultBindArr4()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[3];
            long[] tsArr = new long[3] { 1637064040000, 1637064041000, 1637064042000};
            string[] binaryArr = new string[3] { "abcdABCD123`~!@#$%^&*()-=+_[]{}:;\",.<>/?", String.Empty, null};
            string[] ncharArr = new string[3] { "abcdABCD123`~!@#$%^&*()-=+_[]{}:;\",.<>/?涛思", null, string.Empty };

            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindBinary(binaryArr);
            mBinds[2] = TaosMultiBind.MultiBindNchar(ncharArr);

            return mBinds;
        }

        static void Main(string[] args)
        {
            stmtfunction test = new stmtfunction();
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("Start Stmtfunction case1 insert  Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            //Init and connect TDengine
            test.InitTDengine();
            test.ConnectTDengine();
            //create database
            test.executeQuery("drop database  if exists csharptest");
            test.executeQuery("create database if not exists csharptest ");
            test.executeQuery("use csharptest");
            test.executeQuery("drop table  if exists stmttest");
            //case1:tinyint,smallint,int,bigint
            string createTable1 = "create stable stmttest1 (ts timestamp,c1 tinyint,c2 smallint,c3 int,c4 bigint) tags(t1 tinyint,t2 smallint,t3 int,t4 bigint)";
            test.executeQuery(createTable1);
            test.StmtInit();
            test.StmtPrepare("insert into ? using stmttest1 tags(?,?,?,?) values(?,?,?,?,?)");
            TAOS_BIND[] Ibinds = test.InitBindArr1();
            TAOS_MULTI_BIND[] Imbinds = test.InitMultBindArr1();
            test.SetTableNameTags("t1",Ibinds);
            test.BindParamBatch(Imbinds);
            test.AddBatch();
            test.StmtExecute();
            TaosBind.FreeTaosBind(Ibinds);
            TaosMultiBind.FreeTaosBind(Imbinds);
            test.StmtClose();
            //select
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("start Stmtfunction case1 select Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            test.StmtInit();
            test.StmtPrepare("select * from t1 where c1>? and c2 >?");
            TAOS_BIND[] queryCondition1 = new TAOS_BIND[2];
            queryCondition1[0] = TaosBind.BindTinyInt(0);
            queryCondition1[1] = TaosBind.BindInt(100);
            test.BindParam(queryCondition1);
            test.StmtExecute();
            test.StmtUseResult();
            test.StmtClose();
            TaosBind.FreeTaosBind(queryCondition1);
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("Stop Stmtfunction case1 Testing...");
            Console.WriteLine("---------------------------------------------------------------");

            // //case2:utinyint,usmallint,uint,ubigint
            string createTable2 = "create stable stmttest2 (ts timestamp,c1 tinyint unsigned,c2 smallint unsigned,c3 int unsigned,c4 bigint unsigned)"
                                    +" tags(t1 tinyint unsigned,t2 smallint unsigned,t3 int unsigned,t4 bigint unsigned)";
            test.executeQuery(createTable2);
            test.StmtInit();
            test.StmtPrepare("insert into ? using stmttest2 tags(?,?,?,?) values(?,?,?,?,?)");
            TAOS_BIND[] Ubinds = test.InitBindArr2();
            TAOS_MULTI_BIND[] Umbinds = test.InitMultBindArr2();
            test.SetTableNameTags("t2",Ubinds);
            test.BindParamBatch(Umbinds);
            test.AddBatch();
            test.StmtExecute();
            TaosBind.FreeTaosBind(Ubinds);
            TaosMultiBind.FreeTaosBind(Umbinds);
            test.StmtClose();
            //select
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("start Stmtfunction case2 select Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            test.StmtInit();
            test.StmtPrepare("select * from t2 where c1>? and c3 >?");
            TAOS_BIND[] queryCondition2 = new TAOS_BIND[2];
            queryCondition2[0] = TaosBind.BindUTinyInt(80);
            queryCondition2[1] = TaosBind.BindUInt(1000);
            test.BindParam(queryCondition2);
            test.StmtExecute();
            test.StmtUseResult();
            test.StmtClose();
            TaosBind.FreeTaosBind(queryCondition2);
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("Stop Stmtfunction case2 Testing...");
            Console.WriteLine("---------------------------------------------------------------");


            // //case3:float,double
            string createTable3 = "create stable stmttest3 (ts timestamp,c1 float,c2 double)"
                                    +" tags(t1 float,t2 float,t3 float,t4 double,t5 double,t6 double)";
            test.executeQuery(createTable3);
            test.StmtInit();
            test.StmtPrepare("insert into ? using stmttest3 tags(?,?,?,?,?,?) values(?,?,?)");
            TAOS_BIND[] fdbinds = test.InitBindArr3();
            TAOS_MULTI_BIND[] fdmbinds = test.InitMultBindArr3();
            test.SetTableNameTags("t3",fdbinds);
            test.BindParamBatch(fdmbinds);
            test.AddBatch();
            test.StmtExecute();
            TaosBind.FreeTaosBind(fdbinds);
            TaosMultiBind.FreeTaosBind(fdmbinds);
            test.StmtClose();
            //select
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("start Stmtfunction case3 select Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            test.StmtInit();
            test.StmtPrepare("select * from t3 where c1>? and c2 >?");
            TAOS_BIND[] queryCondition3 = new TAOS_BIND[2];
            queryCondition3[0] = TaosBind.BindFloat(80);
            queryCondition3[1] = TaosBind.BindDouble(1000);
            test.BindParam(queryCondition3);
            test.StmtExecute();
            test.StmtUseResult();
            test.StmtClose();
            TaosBind.FreeTaosBind(queryCondition3);
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("Stop Stmtfunction case3 Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            

            //case4:binary,nchar
            string createTable4 = "create stable stmttest4 (ts timestamp,c1 binary(50),c2 nchar(50))tags(t1 binary(50),t2 nchar(50))";
            //Console.WriteLine(createTable4);
            test.executeQuery(createTable4);
            test.StmtInit();
            test.StmtPrepare("insert into ? using stmttest4 tags(?,?) values(?,?,?)");
            TAOS_BIND[] bnbinds = test.InitBindArr4();
            TAOS_MULTI_BIND[] bnmbinds = test.InitMultBindArr4();
            test.SetTableNameTags("t4",bnbinds);
            test.BindParamBatch(bnmbinds);
            test.AddBatch();
            test.StmtExecute();
            TaosBind.FreeTaosBind(bnbinds);
            TaosMultiBind.FreeTaosBind(bnmbinds);
            test.StmtClose();
            //select
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("start Stmtfunction case4 select Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            test.StmtInit();
            test.StmtPrepare("select * from t4 where c1 match ?");
            TAOS_BIND[] queryCondition4 = new TAOS_BIND[1];
            queryCondition4[0] = TaosBind.BindBinary("\"^a\"");
            
            test.BindParam(queryCondition4);
            test.StmtExecute();
            test.StmtUseResult();
            test.StmtClose();
            TaosBind.FreeTaosBind(queryCondition4);
            Console.WriteLine("---------------------------------------------------------------");
            Console.WriteLine("Stop Stmtfunction case4 Testing...");
            Console.WriteLine("---------------------------------------------------------------");
            test.CloseConnection();

            ExitProgram();

        }

        //Start here are the framework functions
        public void InitTDengine()
        {
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_CONFIGDIR, this.configDir);
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            Console.WriteLine("init...");
            TDengine.Init();
            Console.WriteLine("get connection starting...");
        }

        public void ConnectTDengine()
        {
            string db = "";
            this.conn = TDengine.Connect(host, this.user, this.passwd, db, this.port);
            if (this.conn == IntPtr.Zero)
            {
                Console.WriteLine("connection failed: " + this.host);
                ExitProgramFailed();
            }
            else
            {
                Console.WriteLine("[ OK ] Connection established.");
            }
        }
        public void StmtInit()
        {
            this.stmt = TDengine.StmtInit(conn);
            if (this.stmt == IntPtr.Zero)
            {
                Console.WriteLine("Init stmt failed");
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
            }
        }
        public void executeQuery(String sql)
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
                ExitProgramFailed();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");
            }
            TDengine.FreeResult(res);
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
                    ExitProgramFailed();
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
                ExitProgramFailed();
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
                ExitProgramFailed();
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
            System.Environment.Exit(0);
        }
        public static void ExitProgramFailed()
        {
            TDengine.Cleanup();
            System.Environment.Exit(1);
        }
    }


}
