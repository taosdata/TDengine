using System;
using TDengine.Driver;
using System.Threading;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Benchmark
{
    internal class Batch
    {
        string Host { get; set; }
        ushort Port { get; set; }
        string Username { get; set; }
        string Password { get; set; }
        readonly string db = "benchmark";
        readonly string stb = "stb";
        readonly string jtb = "jtb";
        int MaxSqlLength { get; set; } = 5000;

        int _numOfThreadNotYetCompleted = 1;
        readonly long begineTime = 1659283200000;
        ManualResetEvent _doneEvent = new ManualResetEvent(false);

        public Batch(string host, string userName, string passwd, ushort port, int maxSqlLength)
        {
            Host = host;
            Username = userName;
            Password = passwd;
            Port = port;
            MaxSqlLength = maxSqlLength;
        }

        public void Run(string types, int recordNum, int tableCnt, int loopTime)
        {
            // Console.WriteLine("Insert {0} ... ", types);
            IntPtr conn = NativeMethods.Connect(Host, Username, Password, db, Port);
            IntPtr res;
            if (conn != IntPtr.Zero)
            {
                res = NativeMethods.Query(conn, $"use {db}");
                IfTaosQuerySucc(res, $"use {db}");
                NativeMethods.FreeResult(res);

                if (types == "normal")
                {
                    InsertLoop(conn, tableCnt, recordNum, stb, loopTime);
                }

                if (types == "json")
                {
                    InsertLoop(conn, tableCnt, recordNum, jtb, loopTime);
                }
            }
            else
            {
                throw new Exception("create TD connection failed");
            }

            NativeMethods.Close(conn);
            Console.WriteLine("======TDengineDriver.TDengine.Close(conn);");
        }

        public void InsertLoop(IntPtr conn, int tableCnt, int recordCnt, string prefix, int times)
        {
            _numOfThreadNotYetCompleted = tableCnt;
            for (int i = 0; i < tableCnt; i++)
            {
                Console.WriteLine(i);
                RunContext context = new RunContext($"{prefix}_{i}", recordCnt, tableCnt, conn);
                Console.WriteLine(context.Display());
                ThreadPool.QueueUserWorkItem(RunBatchInsertSql, context);
            }

            _doneEvent.WaitOne();
        }


        public void RunBatchInsertSql(object status)
        {
            RunContext context = (RunContext)status;
            try
            {
                int numOfRecord = context.numOfRows;
                InsertGenerator generator = new InsertGenerator(begineTime, MaxSqlLength);
                while (numOfRecord > 0)
                {
                    int tmpRecords = Math.Min(MaxSqlLength, numOfRecord);
                    string sql = generator.RandomSQL(context.tableName, tmpRecords);
                    // Console.WriteLine(sql);
                    IntPtr res = NativeMethods.Query(context.conn, sql);
                    IfTaosQuerySucc(res, sql);
                    numOfRecord -= tmpRecords;
                    NativeMethods.FreeResult(res);
                }
            }
            finally
            {
                if (Interlocked.Decrement(ref _numOfThreadNotYetCompleted) == 0)
                    _doneEvent.Set();
            }
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