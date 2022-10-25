using TDengineDriver;

namespace TDengineExample
{
    internal class InfluxDBLineExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            PrepareDatabase(conn);
            string[] lines = {
                "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
                "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
                "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
                "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250"
            };
            IntPtr res = TDengine.SchemalessInsert(conn, lines, lines.Length, (int)TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("SchemalessInsert failed since " + TDengine.Error(res));
            }
            else
            {
                int affectedRows = TDengine.AffectRows(res);
                Console.WriteLine($"SchemalessInsert success, affected {affectedRows} rows");
            }
            TDengine.FreeResult(res);

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

        static void PrepareDatabase(IntPtr conn)
        {
            IntPtr res = TDengine.Query(conn, "CREATE DATABASE test");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to create database, reason: " + TDengine.Error(res));
            }
            res = TDengine.Query(conn, "USE test");
            if (TDengine.ErrorNo(res) != 0)
            {
                throw new Exception("failed to change database, reason: " + TDengine.Error(res));
            }
        }

    }

}
