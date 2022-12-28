using System;
using TDengineWS.Impl;

namespace Examples
{
    public class WSConnExample
    {
        static int Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
  
            if (wsConn == IntPtr.Zero)
            {
                Console.WriteLine("get WS connection failed");
                return -1;
            }
            else
            {
                Console.WriteLine("Establish connect success.");
                // close connection.
                LibTaosWS.WSClose(wsConn);
            }

            return 0;
        }
    }
}
