using System;
using TDengine.Driver;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Benchmark
{
    internal class Connect
    {
        string Host { get; set; }
        ushort Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        string db = "benchmark";

        public Connect(string host, string userName, string passwd, ushort port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }
        public void Run(int times)
        {
            Console.WriteLine("Connection ... ");
            int i = 0;
            while (i < times)
            {
                IntPtr conn = NativeMethods.Connect(Host, Username, Password, db, Port);
                if (conn != IntPtr.Zero)
                {
                    NativeMethods.Close(conn);
                }
                else
                {
                    throw new Exception("create TD connection failed");
                }
                i++;
            }
            Console.WriteLine(" time:{0} done ...", i);
        }
    }
}
