using TDengineDriver;

namespace TDengineExample
{
    internal class StmtInsertExample
    {
        private static IntPtr stmt;
        static void Main()
        {
            IntPtr conn = GetConnection();
            PrepareSTable(conn);
            // 1. init and prepare
            stmt = TDengine.StmtInit(conn);
            if (stmt == IntPtr.Zero) {
                Console.WriteLine("failed to init stmt, " + TDengine.Error(stmt));
                ExitProgram();
            }
            int res = TDengine.StmtPrepare(stmt, "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)");
            CheckResInt(res, "failed to prepare stmt");

            // 2. bind table name and tags
            TAOS_BIND[] tags =  new TAOS_BIND[2] {TaosBind.BindBinary("Beijing.Chaoyang"), TaosBind.BindInt(2) };
            res = TDengine.StmtSetTbnameTags(stmt, "d1001", tags);
            CheckResInt(res, "failed to bind table name and tags");

            // 3. bind values
            TAOS_MULTI_BIND[] values = new TAOS_MULTI_BIND[4] {
                TaosMultiBind.MultiBindTimestamp(new long[2] { 1648432611249, 1648432611749}),
                TaosMultiBind.MultiBindFloat(new float?[2] { 10.3f, 12.6f}),
                TaosMultiBind.MultiBindInt(new int?[2] { 219, 218}),
                TaosMultiBind.MultiBindFloat(new float?[2]{ 0.31f, 0.33f})
            };
            res = TDengine.StmtBindParamBatch(stmt, values);
            CheckResInt(res, "failed to bind params");

            // 4. add batch
            res = TDengine.StmtAddBatch(stmt);
            CheckResInt(res, "failed to add batch");
            
            // 5. execute
            res = TDengine.StmtExecute(stmt);
            CheckResInt(res, "faild to execute");

            // 6. free 
            TaosBind.FreeTaosBind(tags);
            TaosMultiBind.FreeTaosBind(values);
            TDengine.Close(conn);
            Console.WriteLine("done!");

            TDengine.Cleanup();
        }

        static IntPtr GetConnection()
        {
            string host = "localhost";
            short port = 6030;
            string username = "root";
            string password = "taosdata";
            string dbname = "";
            var conn = TDengine.Connect(host, username, password, dbname, port);
            if (conn == IntPtr.Zero)
            {
                Console.WriteLine("Connect to TDengine failed");
                System.Environment.Exit(0);
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

   

        static void PrepareSTable(IntPtr conn) {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE power");
            CheckResPtr(res, "failed to create database");
            res = TDengine.Query(conn, "USE power");
            CheckResPtr(res, "failed to change database");
            res = TDengine.Query(conn, "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
            CheckResPtr(res, "failed to create stable");
        }

        static void CheckResInt(int res, String errorMsg)
        {
            if (res != 0)
            {
                Console.WriteLine(errorMsg + ", " + TDengine.StmtErrorStr(stmt));
                TDengine.StmtClose(stmt);
                ExitProgram();
            }
        }

        static void CheckResPtr(IntPtr res, String errorMsg)
        {
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                Console.Write(errorMsg);
                if (res != IntPtr.Zero)
                {
                    Console.Write(", reason: " + TDengine.Error(res));
                }
                Console.WriteLine("");
                ExitProgram();
            }
        }

        static void ExitProgram()
        {
            TDengine.Cleanup();
            System.Environment.Exit(1);
        }
    }
}
