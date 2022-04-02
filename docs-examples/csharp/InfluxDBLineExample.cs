using TDengineDriver;

namespace TDengineExample
{
    internal class InfluxDBLineExample
    {
        static void Main() {
            IntPtr conn = GetConnection();
            PrepareDatabase(conn);
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

        static void PrepareDatabase(IntPtr conn)
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE test");
            CheckResPtr(res, "failed to create database");
            res = TDengine.Query(conn, "USE test");
            CheckResPtr(res, "failed to change database");
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
