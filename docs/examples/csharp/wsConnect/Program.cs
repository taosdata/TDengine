using System;
using TDengineWS.Impl;

namespace Examples
{
    public class WSConnExample
    {
        static void Main(string[] args)
        {
            string DSN = "ws://root:taosdata@127.0.0.1:6041/test";
            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(DSN);
  
            if (wsConn == IntPtr.Zero)
            {
                throw new Exception("get WS connection failed");
            }
            else
            {
                Console.WriteLine("Establish connect success.");
            }

            // close connection.
            LibTaosWS.WSClose(wsConn);
        }
    }
}