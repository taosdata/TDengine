using System;
using TDengine.Driver;
using TDengine.Driver.Impl;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Benchmark
{
    internal class Query
    {
        string Host { get; set; }
        ushort Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        readonly string db = "benchmark";
        readonly string queryStb = "select * from stb;";

        readonly string queryJtb = "select " +
                                   "ts " +
                                   ",bl " +
                                   ",i8 " +
                                   ",i16" +
                                   ",i32" +
                                   ",i64" +
                                   ",u8 " +
                                   ",u16" +
                                   ",u32" +
                                   ",u64" +
                                   ",f32" +
                                   ",d64" +
                                   ",bnr" +
                                   ",nchr" +
                                   ",jtag->\"k0\"" +
                                   ",jtag->\"k1\"" +
                                   ",jtag->\"k2\"" +
                                   ",jtag->\"k3\"" +
                                   "from jtb;";


        public Query(string host, string userName, string passwd, ushort port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }

        public void Run(string types, int times)
        {
            // Console.WriteLine("Query {0} ... ", types);
            IntPtr conn = NativeMethods.Connect(Host, Username, Password, db, Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = NativeMethods.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                NativeMethods.FreeResult(res);

                if (types == "normal")
                {
                    QueryLoop(conn, times, queryStb);
                }

                if (types == "json")
                {
                    QueryLoop(conn, times, queryJtb);
                }
            }
            else
            {
                throw new Exception("create TD connection failed");
            }

            NativeMethods.Close(conn);
        }

        public void QueryLoop(IntPtr conn, int times, string sql)
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
            // Console.WriteLine("last time:{0}", i);
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