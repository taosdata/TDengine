using System;
using TDengine.Driver.Impl.NativeMethods;

namespace Benchmark
{
    internal class Aggregate
    {
        string Host { get; set; }
        ushort Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        readonly string db = "benchmark";
        readonly string avgStb = "select avg(d64) from stb;";
        readonly string avgJtb = "select avg(d64) from jtb;";


        public Aggregate(string host, string userName, string passwd, ushort port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }

        public void Run(string types, int times)
        {
            //Console.WriteLine("Aggregate {0} ...", types);

            IntPtr conn = NativeMethods.Connect(Host, Username, Password, db, Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = NativeMethods.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                NativeMethods.FreeResult(res);

                if (types == "normal")
                {
                    AggregateLoop(conn, times, avgStb);
                }

                if (types == "json")
                {
                    AggregateLoop(conn, times, avgJtb);
                }
            }
            else
            {
                throw new Exception("create TD connection failed");
            }

            NativeMethods.Close(conn);
        }

        public void AggregateLoop(IntPtr conn, int times, string sql)
        {
            IntPtr res;
            int i = 0;
            while (i < times)
            {
                res = NativeMethods.Query(conn, sql);
                IfTaosQuerySucc(res, sql);
                NativeMethods.FetchFields(res);
                Tools.GetData(res);
                NativeMethods.FreeResult(res);
                i++;
            }
            //Console.WriteLine("last time:{0}", i);
        }

        public bool IfTaosQuerySucc(IntPtr res, string sql)
        {
            if (NativeMethods.ErrorNo(res) == 0)
            {
                return true;
            }
            else
            {
                throw new Exception(
                    $"execute {sql} failed,reason {NativeMethods.Error(res)}, code{NativeMethods.ErrorNo(res)}");
            }
        }
    }
}