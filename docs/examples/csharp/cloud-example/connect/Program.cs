using System;
using TDengineWS.Impl;

namespace Cloud.Examples
{
    public class ConnectExample
    {
        static void Main(string[] args)
        {
            string dsn = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_DSN");
            Connect(dsn);
        }

        public static void Connect(string dsn)
        {
            // get connect
            IntPtr conn = LibTaosWS.WSConnectWithDSN(dsn);
            if (conn == IntPtr.Zero)
            {
                throw new Exception($"get connection failed,reason:{LibTaosWS.WSErrorStr(conn)},code:{LibTaosWS.WSErrorNo(conn)}");
            }
            else
            {
                Console.WriteLine("Establish connect success.");
            }

            // do something ...

            // close connect
            LibTaosWS.WSClose(conn);

        }
    }
}