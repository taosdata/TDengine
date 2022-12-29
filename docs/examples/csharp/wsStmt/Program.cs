using System;
using TDengineWS.Impl;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace Examples
{
    public class WSStmtExample
    {
        static int Main(string[] args)
        {
            const string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            const string table = "meters";
            const string database = "test";
            const string childTable = "d1005";
            string insert = $"insert into ? using {database}.{table} tags(?,?) values(?,?,?,?)";
            const int numOfTags = 2;
            const int numOfColumns = 4;

            // Establish connection
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine($"get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success...");
            }

            // init stmt
            IntPtr wsStmt = LibTaosWS.WSStmtInit(wsConn);
            if (wsStmt != IntPtr.Zero)
            {
                int code = LibTaosWS.WSStmtPrepare(wsStmt, insert);
                ValidStmtStep(code, wsStmt, "WSStmtPrepare");

                TAOS_MULTI_BIND[] wsTags = new TAOS_MULTI_BIND[] { WSMultiBind.WSBindNchar(new string[] { "California.SanDiego" }), WSMultiBind.WSBindInt(new int?[] { 4 }) };
                code = LibTaosWS.WSStmtSetTbnameTags(wsStmt, $"{database}.{childTable}", wsTags, numOfTags);
                ValidStmtStep(code, wsStmt, "WSStmtSetTbnameTags");

                TAOS_MULTI_BIND[] data = new TAOS_MULTI_BIND[4];
                data[0] = WSMultiBind.WSBindTimestamp(new long[] { 1538548687000, 1538548688000, 1538548689000, 1538548690000, 1538548691000 });
                data[1] = WSMultiBind.WSBindFloat(new float?[] { 10.30F, 10.40F, 10.50F, 10.60F, 10.70F });
                data[2] = WSMultiBind.WSBindInt(new int?[] { 223, 221, 222, 220, 219 });
                data[3] = WSMultiBind.WSBindFloat(new float?[] { 0.31F, 0.32F, 0.33F, 0.35F, 0.28F });
                code = LibTaosWS.WSStmtBindParamBatch(wsStmt, data, numOfColumns);
                ValidStmtStep(code, wsStmt, "WSStmtBindParamBatch");

                code = LibTaosWS.WSStmtAddBatch(wsStmt);
                ValidStmtStep(code, wsStmt, "WSStmtAddBatch");

                IntPtr stmtAffectRowPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
                code = LibTaosWS.WSStmtExecute(wsStmt, stmtAffectRowPtr);
                ValidStmtStep(code, wsStmt, "WSStmtExecute");
                Console.WriteLine("WS STMT insert {0} rows...", Marshal.ReadInt32(stmtAffectRowPtr));
                Marshal.FreeHGlobal(stmtAffectRowPtr);

                LibTaosWS.WSStmtClose(wsStmt);

                // Free unmanaged memory
                WSMultiBind.WSFreeTaosBind(wsTags);
                WSMultiBind.WSFreeTaosBind(data);

                //check result with SQL "SELECT * FROM test.d1005;"
            }
            else
            {
                Console.WriteLine("Init STMT failed...");
            }

            // close connection.
            LibTaosWS.WSClose(wsConn);

            return 0;
        }

        static void ValidStmtStep(int code, IntPtr wsStmt, string desc)
        {
            if (code != 0)
            {
                Console.WriteLine($"{desc} failed,reason: {LibTaosWS.WSErrorStr(wsStmt)}, code: {code}");
            }
            else
            {
                Console.WriteLine("{0} success...", desc);
            }
        }
    }
}

// WSStmtPrepare success...
// WSStmtSetTbnameTags success...
// WSStmtBindParamBatch success...
// WSStmtAddBatch success...
// WSStmtExecute success...
// WS STMT insert 5 rows...
