using TDengineDriver;

namespace TDengineExample
{
    internal class OptsTelnetExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            PrepareDatabase(conn);
            string[] lines = {
                "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
                "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
                "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
                "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
                "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
                "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
                "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
                "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
            };
            IntPtr res = TDengine.SchemalessInsert(conn, lines, lines.Length, (int)TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.WriteLine("SchemalessInsert failed since " + TDengine.Error(res));
                ExitProgram(conn, 1);
            }
            else
            {
                int affectedRows = TDengine.AffectRows(res);
                Console.WriteLine($"SchemalessInsert success, affected {affectedRows} rows");
            }
            TDengine.FreeResult(res);
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
                TDengine.Cleanup();
                Environment.Exit(1);
            }
            else
            {
                Console.WriteLine("Connect to TDengine success");
            }
            return conn;
        }

        static void PrepareDatabase(IntPtr conn)
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE test");
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.WriteLine("failed to create database, reason: " + TDengine.Error(res));
                ExitProgram(conn, 1);
            }
            res = TDengine.Query(conn, "USE test");
            if (TDengine.ErrorNo(res) != 0)
            {
                Console.WriteLine("failed to change database, reason: " + TDengine.Error(res));
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
