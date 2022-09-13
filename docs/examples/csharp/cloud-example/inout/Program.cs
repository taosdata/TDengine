using System;
using TDengineDriver;
using TDengineWS.Impl;
using System.Collections.Generic;

namespace Cloud.Examples
{
    public class InOutExample
    {
        static void Main(string[] args)
        {
            string dsn = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_DSN");
            IntPtr conn = LibTaosWS.WSConnectWithDSN(dsn);
            if (conn != IntPtr.Zero)
            {

                try
                {
                    // ANCHOR: insert
                    string createTable = "CREATE STABLE if not exists test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)";
                    string insertData = "INSERT INTO test.d1001 USING test.meters TAGS('California.SanFrancisco', 1) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000)" +
                                        "test.d1002 USING test.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)" +
                                        "test.d1003 USING test.meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000)" +
                                        "test.d1004 USING test.meters TAGS('California.LosAngeles', 4) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ";

                    // create database under database named 'test'
                    IntPtr res = LibTaosWS.WSQuery(conn, createTable);
                    ValidQueryExecution(res);
                    // Free the query result every time when used up it.
                    LibTaosWS.WSFreeResult(res);

                    // insert data into the table created in previous step.
                    res = LibTaosWS.WSQuery(conn, insertData);
                    ValidQueryExecution(res);
                    // Free the query result every time when used up it.
                    LibTaosWS.WSFreeResult(res);
                    // ANCHOR: query
                    void ValidQueryExecution(IntPtr res)
                    {
                        int code = LibTaosWS.WSErrorNo(res);
                        if (code != 0)
                        {
                            throw new Exception($"execute SQL failed: reason: {LibTaosWS.WSErrorStr(res)}, code:{code}");
                        }
                    }
                    // ANCHOR_END: insert
                    string selectTable = "select * from test.meters";
                    res = LibTaosWS.WSQueryTimeout(conn, selectTable, 5000);
                    ValidQueryExecution(res);

                    // get meta info of the retrieved data as List
                    List<TDengineMeta> metas = LibTaosWS.WSGetFields(res);

                    // get data of the retrieved data as List.
                    List<object> dataSet = LibTaosWS.WSGetData(res);

                    // Free the query result every time when used up it.
                    LibTaosWS.WSFreeResult(res);
                    // ANCHOR_END: query
                }
                finally
                {
                    // close connect
                    LibTaosWS.WSClose(conn);
                }
            }
            else
            {
                throw new Exception($"get connection failed,reason:{LibTaosWS.WSErrorStr(conn)},code:{LibTaosWS.WSErrorNo(conn)}");
            }
        }

    }
}