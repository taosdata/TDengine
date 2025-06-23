using System;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Benchmark
{
    internal class Stmt
    {
        string Host { get; set; }
        ushort Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        string db = "benchmark";
        string sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        string jtable = "jtb_1";
        string stable = "stb_1";

        public Stmt(string host, string userName, string passwd, ushort port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }

        public void Run(string types, int times)
        {
            IntPtr res;
            IntPtr conn = NativeMethods.Connect(Host, Username, Password, db, Port);
            if (conn != IntPtr.Zero)
            {
                res = NativeMethods.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                NativeMethods.FreeResult(res);

                // begin stmt
                IntPtr stmt = NativeMethods.StmtInit(conn);
                if (stmt != IntPtr.Zero)
                {
                    int i = 0;
                    while (i < times)
                    {
                        if (types == "normal")
                        {
                            StmtBindBatch(stmt, sql, stable);
                        }

                        if (types == "json")
                        {
                            StmtBindBatch(stmt, sql, jtable);
                        }

                        i++;
                    }
                }
                else
                {
                    throw new Exception("init stmt failed.");
                }

                NativeMethods.StmtClose(stmt);
            }
            else
            {
                throw new Exception("create TD connection failed");
            }

            NativeMethods.Close(conn);
        }

        public void StmtBindBatch(IntPtr stmt, string sql, string table)
        {
            int stmtRes = NativeMethods.StmtPrepare(stmt, sql);
            IfStmtSucc(stmtRes, stmt, "StmtPrepare");

            stmtRes = NativeMethods.StmtSetTbname(stmt, table);
            IfStmtSucc(stmtRes, stmt, "StmtSetTbname");

            TAOS_MULTI_BIND[] dataBind = StmtData();

            stmtRes = NativeMethods.StmtBindParamBatch(stmt, dataBind);
            IfStmtSucc(stmtRes, stmt, "StmtBindParamBatch");

            stmtRes = NativeMethods.StmtAddBatch(stmt);
            IfStmtSucc(stmtRes, stmt, "StmtAddBatch");

            stmtRes = NativeMethods.StmtExecute(stmt);
            IfStmtSucc(stmtRes, stmt, "StmtExecute");

            foreach (var bind in dataBind)
            {
                MultiBind.FreeTaosBind(bind);
            }
        }

        public bool IfTaosQuerySucc(IntPtr res, string sql)
        {
            if (NativeMethods.ErrorNo(res) == 0)
            {
                return true;
            }
            else
            {
                throw new Exception(
                    $"execute {sql} failed,reason {NativeMethods.Error(res)}, code{NativeMethods.ErrorNo(res)}");
            }
        }

        public void IfStmtSucc(int stmtReturn, IntPtr stmt, string method)
        {
            if (stmtReturn != 0)
            {
                throw new Exception($"{method} failed,reason:{NativeMethods.StmtErrorStr(stmt)}");
            }
        }

        public TAOS_MULTI_BIND[] StmtData()
        {
            var mBinds = new TAOS_MULTI_BIND[14];

            long[] tsArr = new long[1] { 1659283200000 };
            bool?[] boolArr = new bool?[1] { true };
            sbyte?[] tinyIntArr = new sbyte?[1] { -1 };
            short?[] shortArr = new short?[1] { -2 };
            int?[] intArr = new int?[1] { -3 };
            long?[] longArr = new long?[1] { -4 };
            byte?[] uTinyIntArr = new byte?[1] { 1 };
            ushort?[] uShortArr = new ushort?[1] { 2 };
            uint?[] uIntArr = new uint?[1] { 3 };
            ulong?[] uLongArr = new ulong?[1] { 4 };
            float?[] floatArr = new float?[1] { 3.1415f };
            double?[] doubleArr = new double?[1] { 3.14159265358979d };
            string[] binaryArr = new string[1] { "bnr_col_1" };
            string[] ncharArr = new string[1] { "ncr_col_1" };

            mBinds[0] = MultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = MultiBind.MultiBindBool(boolArr);
            mBinds[2] = MultiBind.MultiBindTinyInt(tinyIntArr);
            mBinds[3] = MultiBind.MultiBindSmallInt(shortArr);
            mBinds[4] = MultiBind.MultiBindInt(intArr);
            mBinds[5] = MultiBind.MultiBindBigInt(longArr);
            mBinds[6] = MultiBind.MultiBindUTinyInt(uTinyIntArr);
            mBinds[7] = MultiBind.MultiBindUSmallInt(uShortArr);
            mBinds[8] = MultiBind.MultiBindUInt(uIntArr);
            mBinds[9] = MultiBind.MultiBindUBigInt(uLongArr);
            mBinds[10] = MultiBind.MultiBindFloat(floatArr);
            mBinds[11] = MultiBind.MultiBindDouble(doubleArr);
            mBinds[12] = MultiBind.MultiBindStringArray(binaryArr, TDengineDataType.TSDB_DATA_TYPE_BINARY);
            mBinds[13] = MultiBind.MultiBindStringArray(ncharArr, TDengineDataType.TSDB_DATA_TYPE_NCHAR);

            return mBinds;
        }
    }
}