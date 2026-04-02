using System;
using TDengine.Driver;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Benchmark
{
    internal class Prepare
    {
        string Host { get; set; }
        ushort Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        string db = "benchmark";

        readonly string createStb = "create table if not exists stb (ts timestamp" +
                                    ",bl bool" +
                                    ",i8 tinyint" +
                                    ",i16 smallint" +
                                    ",i32 int" +
                                    ",i64 bigint" +
                                    ",u8 tinyint unsigned" +
                                    ",u16 smallint unsigned" +
                                    ",u32 int unsigned" +
                                    ",u64 bigint unsigned" +
                                    ",f32 float" +
                                    ",d64 double" +
                                    ",bnr binary(50)" +
                                    ",nchr nchar(50))" +
                                    "tags(t_bl bool" +
                                    ",t_i8 tinyint" +
                                    ",t_i16 smallint" +
                                    ",t_i32 int" +
                                    ",t_i64 bigint" +
                                    ",t_u8 tinyint unsigned" +
                                    ",t_u16 smallint unsigned" +
                                    ",t_u32 int unsigned" +
                                    ",t_u64 bigint unsigned" +
                                    ",t_f32 float" +
                                    ",t_d64 double" +
                                    ",t_bnr binary(50)" +
                                    ",t_nchr nchar(50));";

        readonly string createJtb = "create table if not exists jtb(ts timestamp" +
                                    ", bl bool" +
                                    ", i8 tinyint" +
                                    ", i16 smallint" +
                                    ", i32 int" +
                                    ", i64 bigint" +
                                    ", u8 tinyint unsigned" +
                                    ", u16 smallint unsigned" +
                                    ", u32 int unsigned" +
                                    ", u64 bigint unsigned" +
                                    ", f32 float" +
                                    ", d64 double" +
                                    ", bnr binary(50)" +
                                    ",nchr nchar(50))" +
                                    "tags(json_tag json);";

        readonly string createStb1 = "create table if not exists stb_2 using stb tags(true" +
                                     ",-1" +
                                     ",-2" +
                                     ",-3" +
                                     ",-4" +
                                     ",1" +
                                     ",2" +
                                     ",3" +
                                     ",4" +
                                     ",3.1415" +
                                     ",3.14159265358979" +
                                     ",'bnr_tag_1'" +
                                     ",'ncr_tag_1');";

        readonly string createJtb1 =
            "create table if not exists jtb_2 using jtb tags('{\"jtag_bool\":false,\"jtag_num\":3.141592653,\"jtag_str\":\"beijing\",\"jtag_null\":null}');";

        public Prepare(string host, string userName, string passwd, ushort port)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
        }

        public void Run(string type)
        {
            // Console.WriteLine("Prepare {0}... ", type);

            IntPtr conn = NativeMethods.Connect(Host, Username, Password, "", 0);

            if (conn != IntPtr.Zero)
            {
                SQLExe(conn, $"create database if not exists {db} keep 3650");
                SQLExe(conn, $"use {db}");

                if (type == "normal")
                {
                    SQLExe(conn, createStb);
                    SQLExe(conn, createStb1);
                }

                if (type == "json")
                {
                    SQLExe(conn, createJtb);
                    SQLExe(conn, createJtb1);
                }
            }
            else
            {
                throw new Exception("create TD connection failed");
            }

            NativeMethods.Close(conn);
        }

        public void SQLExe(IntPtr conn, string sql)
        {
            IntPtr res;

            res = NativeMethods.Query(conn, sql);
            IfTaosQuerySucc(res, sql);
            NativeMethods.FreeResult(res);
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