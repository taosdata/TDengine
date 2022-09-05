using System;
using TDengineWS.Impl;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace Cloud.Examples
{
    public class STMTExample
    {
        static void Main(string[] args)
        {
            string dsn = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_DSN");
            IntPtr conn = Connect(dsn);
            // assume table has been created.
            // CREATE STABLE if not exists test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)
            string insert = "insert into ? using test.meters tags(?,?) values(?,?,?,?)";

            // Init STMT
            IntPtr stmt = LibTaosWS.WSStmtInit(conn);

            if (stmt != IntPtr.Zero)
            {
                // Prepare SQL
                int code = LibTaosWS.WSStmtPrepare(stmt, insert);
                ValidSTMTStep(code, stmt, "WSInit()");

                // Bind child table name and tags
                TAOS_MULTI_BIND[] tags = new TAOS_MULTI_BIND[2] { WSMultiBind.WSBindBinary(new string[] { "California.LosAngeles" }), WSMultiBind.WSBindInt(new int?[] { 6 }) };
                code = LibTaosWS.WSStmtSetTbnameTags(stmt, "test.d1005",tags, 2);
                ValidSTMTStep(code, stmt, "WSStmtSetTbnameTags()");

                // bind column value
                TAOS_MULTI_BIND[] data = new TAOS_MULTI_BIND[4];
                data[0] = WSMultiBind.WSBindTimestamp(new long[] { 1538551000000, 1538552000000, 1538553000000, 1538554000000, 1538555000000 });
                data[1] = WSMultiBind.WSBindFloat(new float?[] { 10.30000F, 10.30000F, 11.30000F, 10.30000F, 10.80000F });
                data[2] = WSMultiBind.WSBindInt(new int?[] { 218, 219, 221, 222, 223 });
                data[3] = WSMultiBind.WSBindFloat(new float?[] { 0.28000F, 0.29000F, 0.30000F, 0.31000F, 0.32000F });
                code = LibTaosWS.WSStmtBindParamBatch(stmt, data, 4);
                ValidSTMTStep(code, stmt, "WSStmtBindParamBatch");

                LibTaosWS.WSStmtAddBatch(stmt);
                ValidSTMTStep(code, stmt, "WSStmtAddBatch");

                IntPtr affectRowPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
                LibTaosWS.WSStmtExecute(stmt, affectRowPtr);
                ValidSTMTStep(code, stmt, "WSStmtExecute");
                Console.WriteLine("STMT affect rows:{0}", Marshal.ReadInt32(affectRowPtr));

                LibTaosWS.WSStmtClose(stmt);

                // Free allocated memory
                Marshal.FreeHGlobal(affectRowPtr);
                WSMultiBind.WSFreeTaosBind(tags);
                WSMultiBind.WSFreeTaosBind(data);
            }
            // close connect
            LibTaosWS.WSClose(conn);
        }

        public static IntPtr Connect(string dsn)
        {
            // get connect
            IntPtr conn = LibTaosWS.WSConnectWithDSN(dsn);
            if (conn == IntPtr.Zero)
            {
                throw new Exception($"get connection failed,reason:{LibTaosWS.WSErrorStr(conn)},code:{LibTaosWS.WSErrorNo(conn)}");
            }
            return conn;
        }

        public static void ValidSTMTStep(int code, IntPtr wsStmt, string method)
        {
            if (code != 0)
            {
                throw new Exception($"{method} failed,reason: {LibTaosWS.WSErrorStr(wsStmt)}, code: {code}");
            }
            else
            {
                Console.WriteLine("{0} success", method);
            }
        }
    }
}