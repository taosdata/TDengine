using System;
using TDengineWS.Impl;
using System.Collections.Generic;
using TDengineDriver;

namespace Examples
{
    public class WSQueryExample
    {
        static void Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
            if (wsConn == IntPtr.Zero)
            {
                throw new Exception($"get WS connection failed,reason:{LibTaosWS.WSErrorStr(IntPtr.Zero)} code:{LibTaosWS.WSErrorNo(IntPtr.Zero)}");
            }
            else
            {
                Console.WriteLine("Establish connect success.");
            }

            string select = "select * from test.meters";

            // optional:wsRes = LibTaosWS.WSQuery(wsConn, select);
            IntPtr wsRes = LibTaosWS.WSQueryTimeout(wsConn, select, 1);
            // Assert if query execute success.
            int code = LibTaosWS.WSErrorNo(wsRes);
            if (code != 0)
            {
                throw new Exception($"execute SQL failed: reason: {LibTaosWS.WSErrorStr(wsRes)}, code:{code}");
            }

            // get meta data
            List<TDengineMeta> metas = LibTaosWS.WSGetFields(wsRes);
            // get retrieved data
            List<object> dataSet = LibTaosWS.WSGetData(wsRes);

            // do something with result.
            foreach (var meta in metas)
            {
                Console.Write("{0} {1}({2}) \t|\t", meta.name, meta.TypeName(), meta.size);
            }
            Console.WriteLine("");

            for (int i = 0; i < dataSet.Count;)
            {
                for (int j = 0; j < metas.Count; j++)
                {
                    Console.Write("{0}\t|\t", dataSet[i]);
                    i++;
                }
                Console.WriteLine("");
            }

            // Free result after use.
            LibTaosWS.WSFreeResult(wsRes);
            
            // close connection.
            LibTaosWS.WSClose(wsConn);
        }
    }
}

// Establish connect success.
// ts TIMESTAMP(8)         |       current FLOAT(4)        |       voltage INT(4)  |       phase FLOAT(4)  |       location BINARY(64)     |       groupid INT(4)  |
// 1538548685000   |       10.8    |       223     |       0.29    |       California.LosAngeles   |       3       |
// 1538548686500   |       11.5    |       221     |       0.35    |       California.LosAngeles   |       3       |
// 1538548685500   |       11.8    |       221     |       0.28    |       California.LosAngeles   |       2       |
// 1538548696600   |       13.4    |       223     |       0.29    |       California.LosAngeles   |       2       |
// 1538548685000   |       10.3    |       219     |       0.31    |       California.SanFrancisco |       2       |
// 1538548695000   |       12.6    |       218     |       0.33    |       California.SanFrancisco |       2       |
// 1538548696800   |       12.3    |       221     |       0.31    |       California.SanFrancisco |       2       |
// 1538548696650   |       10.3    |       218     |       0.25    |       California.SanFrancisco |       3       |