using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Runtime.InteropServices;
using TDengine.Driver;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Test.Fixture
{
    public class DatabaseFixture : IDisposable
    {
        public IntPtr Conn { get; set; }
        readonly string db = "csharp_test";

        public DatabaseFixture()
        {
            string user = "root";
            string password = "taosdata";
            string ip;
            ushort port = 0;


            NativeMethods.Options((int)TDengineInitOption.TSDB_OPTION_CONFIGDIR, GetConfigPath());
            NativeMethods.Options((int)TDengineInitOption.TSDB_OPTION_SHELL_ACTIVITY_TIMER, "90");
            NativeMethods.Options((int)TDengineInitOption.TSDB_OPTION_LOCALE, "C");
            NativeMethods.Options((int)TDengineInitOption.TSDB_OPTION_CHARSET, "UTF-8");
            var ENV_HOST = Environment.GetEnvironmentVariable("TEST_HOST");
            ip = string.IsNullOrEmpty(ENV_HOST) == true ? "127.0.0.1" : ENV_HOST;
            this.Conn = NativeMethods.Connect(ip, user, password, "", port);
            IntPtr res;
            if (Conn != IntPtr.Zero)
            {
                if ((res = NativeMethods.Query(Conn,
                        $"create database if not exists {db} keep 3650 WAL_RETENTION_PERIOD 86400")) != IntPtr.Zero)
                {
                    if ((res = NativeMethods.Query(Conn, $"use {db}")) != IntPtr.Zero)
                    {
                        Console.WriteLine("Get connection success");
                    }
                    else
                    {
                        throw new Exception(NativeMethods.Error(res));
                    }
                }
                else
                {
                    throw new Exception(NativeMethods.Error(res));
                }
            }
            else
            {
                throw new Exception("Get TDConnection failed");
            }
        }

        // public IntPtr TDConnection { get;  }

        public void Dispose()
        {
            //TDengineDriver.TDengine.Close(Conn);

            IntPtr res = IntPtr.Zero;
            if (Conn != IntPtr.Zero)
            {
                res = NativeMethods.Query(Conn, $"drop database if exists {db}");
                if (res != IntPtr.Zero)
                {
                    NativeMethods.Close(Conn);
                    Console.WriteLine("close connection success");
                }
                else
                {
                    NativeMethods.Close(Conn);
                    throw new Exception(NativeMethods.Error(res));
                }
            }
            else
            {
                throw new Exception("connection if already null");
            }
        }

        private string GetConfigPath()
        {
            string configDir = "";
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                configDir = "C:/TDengine/cfg";
            }
            else if (Environment.OSVersion.Platform == PlatformID.Unix)
            {
                configDir = "/etc/taos";
            }
            else if (Environment.OSVersion.Platform == PlatformID.MacOSX)
            {
                configDir = "/usr/local/etc/taos";
            }

            return configDir;
        }
    }
}