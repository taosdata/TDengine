using System;
using TDengine.Driver;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Benchmark
{
    internal class CleanUp
    {
        string Host { get; set; }
        ushort Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        string dropDb = $"drop database if exists benchmark";

        public CleanUp(string host, string userName, string passwd, ushort port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }
        public void Run()
        {
            //Console.WriteLine("cleanup ...");

            IntPtr conn = NativeMethods.Connect(Host, Username, Password, "", Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = NativeMethods.Query(conn, dropDb);
                IfTaosQuerySucc(res, dropDb);
                NativeMethods.FreeResult(res);
            }
            else
            {
                throw new Exception("create TD connection failed");
            }
            NativeMethods.Close(conn);
        }

        public bool IfTaosQuerySucc(IntPtr res, string sql)
        {
            if (NativeMethods.ErrorNo(res) == 0)
            {
                return true;
            }
            else
            {
                throw new Exception($"execute {sql} failed,reason {NativeMethods.Error(res)}, code{NativeMethods.ErrorNo(res)}");
            }
        }
    }
}
