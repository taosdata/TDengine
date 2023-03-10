using TDengineDriver;

namespace TDengineExample
{
    internal class OptsJsonExample
    {
        static void Main()
        {
            IntPtr conn = GetConnection();
            try
            {
                PrepareDatabase(conn);
                string[] lines = { "[{\"metric\": \"meters.current\", \"timestamp\": 1648432611249, \"value\": 10.3, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"meters.voltage\", \"timestamp\": 1648432611249, \"value\": 219, \"tags\": {\"location\": \"California.LosAngeles\", \"groupid\": 1}}, " +
                "{\"metric\": \"meters.current\", \"timestamp\": 1648432611250, \"value\": 12.6, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": 2}}," +
                " {\"metric\": \"meters.voltage\", \"timestamp\": 1648432611250, \"value\": 221, \"tags\": {\"location\": \"California.LosAngeles\", \"groupid\": 1}}]"
            };

                IntPtr res = TDengine.SchemalessInsert(conn, lines, 1, (int)TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL, (int)TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
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
