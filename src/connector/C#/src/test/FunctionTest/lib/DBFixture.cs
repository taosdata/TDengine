using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Runtime.InteropServices;
using TDengineDriver;

namespace Test.Fixture
{
    public class DatabaseFixture : IDisposable
    {
        public IntPtr conn { get; set; }

        private string user = "root";
        private string password = "taosdata";
        private string ip = "127.0.0.1";
        private short port = 0;

        private string db = "xunit_test_fixture";
        public DatabaseFixture()
        {
            conn = TDengine.Connect(ip, user, password, "", port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                if ((res = TDengine.Query(conn, $"create database if not exists {db} keep 3650")) != IntPtr.Zero)
                {
                    if ((res = TDengine.Query(conn, $"use {db}")) != IntPtr.Zero)
                    {
                        Console.WriteLine("Get connection success");
                    }
                    else
                    {
                        throw new Exception(TDengine.Error(res));
                    }
                }
                else
                {
                    throw new Exception(TDengine.Error(res));
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
                IntPtr res;
                if (conn != IntPtr.Zero)
                {
                    if ((res = TDengine.Query(conn, $"drop database if exists {db}")) != IntPtr.Zero)
                    {
                        if (TDengine.Close(conn) == 0)
                        {
                            Console.WriteLine("close connection success");
                        }
                        else
                        {
                            throw new Exception("close connection failed");
                        }

                    }
                    else
                    {
                        throw new Exception(TDengine.Error(res));
                    }
                }
                else
                {
                    throw new Exception("connection if already null");
                }

        }

    }
}
