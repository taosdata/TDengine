using TDengineDriver;


namespace TDengineExample
{
    internal class SQLInsertExample
    {

        static void Main()
        {
            IntPtr conn = GetConnection();
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE power");
            CheckRes(conn, res, "failed to create database");
            res = TDengine.Query(conn, "USE power");
            CheckRes(conn, res, "failed to change database");
            res = TDengine.Query(conn, "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
            CheckRes(conn, res, "failed to create stable");
            var sql = "INSERT INTO d1001 USING meters TAGS(California.SanFrancisco, 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000) " +
                        "d1002 USING power.meters TAGS(California.SanFrancisco, 3) VALUES('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000) " +
                        "d1003 USING power.meters TAGS(California.LosAngeles, 2) VALUES('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000)('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000) " +
                        "d1004 USING power.meters TAGS(California.LosAngeles, 3) VALUES('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000)('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)";
            res = TDengine.Query(conn, sql);
            CheckRes(conn, res, "failed to insert data");
            int affectedRows = TDengine.AffectRows(res);
            Console.WriteLine("affectedRows " + affectedRows);
            ExitProgram(conn, 0);
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
                Environment.Exit(0);
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void CheckRes(IntPtr conn, IntPtr res, String errorMsg)
        {
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.Write(errorMsg + " since: " + TDengine.Error(res));
                ExitProgram(conn, 1);
            }
        }

        static void ExitProgram(IntPtr conn, int exitCode)
        {
            TDengine.Close(conn);
            TDengine.Cleanup();
            Environment.Exit(exitCode);
        }
    }
}

// output:
// Connect to TDengine success
// affectedRows 8
