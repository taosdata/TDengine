using System;
using TDengineWS.Impl;

namespace Examples
{
    public class WSInsertExample
    {
        static int Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);

            // Assert if connection is validate
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine("get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success.");
            }

            string createTable = "CREATE STABLE test.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);";
            string insert = "INSERT INTO test.d1001 USING test.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)" +
                            "test.d1002 USING test.meters TAGS('California.SanFrancisco', 3) VALUES('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)" +
                            "test.d1003 USING test.meters TAGS('California.LosAngeles', 2) VALUES('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000)('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000) " +
                            "test.d1004 USING test.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000)('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)";

            IntPtr wsRes = LibTaosWS.WSQuery(wsConn, createTable);
            ValidInsert("create table", wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            wsRes = LibTaosWS.WSQuery(wsConn, insert);
            ValidInsert("insert data", wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            // close connection.
            LibTaosWS.WSClose(wsConn);

            return 0;
        }

        static void ValidInsert(string desc, IntPtr wsRes)
        {
            int code = LibTaosWS.WSErrorNo(wsRes);
            if (code != 0)
            {
                Console.WriteLine($"execute SQL failed: reason: {LibTaosWS.WSErrorStr(wsRes)}, code:{code}");
            }
            else
            {
                Console.WriteLine("{0} success affect {2} rows, cost {1} nanoseconds", desc, LibTaosWS.WSTakeTiming(wsRes), LibTaosWS.WSAffectRows(wsRes));
            }
        }
    }

}
// Establish connect success.
// create table success affect 0 rows, cost 3717542 nanoseconds
// insert data success affect 8 rows, cost 2613637 nanoseconds
