using TDengineDriver;

namespace TDengineExample
{
    internal class StmtInsertExample
    {
        private static IntPtr conn;
        private static IntPtr stmt;
        static void Main()
        {
            conn = GetConnection();
            try
            {
                PrepareSTable();
                // 1. init and prepare
                stmt = TDengine.StmtInit(conn);
                if (stmt == IntPtr.Zero)
                {
                    throw new Exception("failed to init stmt.");
                }
                int res = TDengine.StmtPrepare(stmt, "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)");
                CheckStmtRes(res, "failed to prepare stmt");

                // 2. bind table name and tags
                TAOS_MULTI_BIND[] tags = new TAOS_MULTI_BIND[2] { TaosMultiBind.MultiBindBinary(new string[] { "California.SanFrancisco" }), TaosMultiBind.MultiBindInt(new int?[] { 2 }) };
                res = TDengine.StmtSetTbnameTags(stmt, "d1001", tags);
                CheckStmtRes(res, "failed to bind table name and tags");

                // 3. bind values
                TAOS_MULTI_BIND[] values = new TAOS_MULTI_BIND[4] {
                TaosMultiBind.MultiBindTimestamp(new long[2] { 1648432611249, 1648432611749}),
                TaosMultiBind.MultiBindFloat(new float?[2] { 10.3f, 12.6f}),
                TaosMultiBind.MultiBindInt(new int?[2] { 219, 218}),
                TaosMultiBind.MultiBindFloat(new float?[2]{ 0.31f, 0.33f})
            };
                res = TDengine.StmtBindParamBatch(stmt, values);
                CheckStmtRes(res, "failed to bind params");

                // 4. add batch
                res = TDengine.StmtAddBatch(stmt);
                CheckStmtRes(res, "failed to add batch");

                // 5. execute
                res = TDengine.StmtExecute(stmt);
                CheckStmtRes(res, "faild to execute");

                // 6. free 
                TaosMultiBind.FreeTaosBind(tags);
                TaosMultiBind.FreeTaosBind(values);
            }
            finally
            {
                TDengine.Close(conn);
            }

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
                throw new Exception("Connect to TDengine failed");
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void PrepareSTable()
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE power");
            CheckResPtr(res, "failed to create database");
            res = TDengine.Query(conn, "USE power");
            CheckResPtr(res, "failed to change database");
            res = TDengine.Query(conn, "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
            CheckResPtr(res, "failed to create stable");
        }

        static void CheckStmtRes(int res, string errorMsg)
        {
            if (res != 0)
            {
                Console.WriteLine(errorMsg + ", " + TDengine.StmtErrorStr(stmt));
                int code = TDengine.StmtClose(stmt);
                if (code != 0)
                {
                    throw new Exception($"falied to close stmt, {code} reason: {TDengine.StmtErrorStr(stmt)} ");
                }
            }
        }

        static void CheckResPtr(IntPtr res, string errorMsg)
        {
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception(errorMsg + " since:" + TDengine.Error(res));
            }
        }

    }
}
