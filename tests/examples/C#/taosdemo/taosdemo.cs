/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Collections;
using System.Threading;
using System.Diagnostics;

namespace TDengineDriver
{
    class TDengineTest
    {
        //connect parameters
        private string host = "127.0.0.1";
        private string configDir = "C:/TDengine/cfg";
        private string user = "root";
        private string password = "taosdata";
        private short port = 0;

        //sql parameters
        private string dbName = "db";
        private string stableName = "st";
        private string tablePrefix = "t";

        private bool isInsertOnly = false;
        private int queryMode = 1;

        private long recordsPerTable = 1;
        private int recordsPerRequest = 1;
        private int colsPerRecord = 3;
        private long batchRows = 1000;
        private long numOfTables = 1;
        private short replica = 1;

        private IntPtr conn = IntPtr.Zero;
        //        private long rowsInserted = 0;
        private bool useStable = false;
        private short methodOfDelete = 0;
        private long numOfThreads = 1;
        private long rateOfOutorder = 0;
        private bool order = true;
        private bool skipReadKey = false;
        private bool verbose = false;


        static void PrintHelp(String[] argv)
        {
            for (int i = 0; i < argv.Length; ++i)
            {
                if ("--help" == argv[i])
                {
                    Console.WriteLine("Usage: mono taosdemo.exe [OPTION...]");
                    Console.WriteLine("");
                    string indent = "        ";
                    Console.WriteLine("{0}{1}", indent, "--help            Show usage.");
                    Console.WriteLine("");
                    Console.Write("{0}{1}", indent, "-h");
                    Console.Write("{0}{1}{2}\n", indent, indent, "host, The host to connect to TDengine. Default is localhost.");
                    Console.Write("{0}{1}", indent, "-p");
                    Console.Write("{0}{1}{2}\n", indent, indent, "port, The TCP/IP port number to use for the connection. Default is 0.");
                    Console.Write("{0}{1}", indent, "-u");
                    Console.Write("{0}{1}{2}\n", indent, indent, "user, The user name to use when connecting to the server. Default is 'root'.");
                    Console.Write("{0}{1}", indent, "-P");
                    Console.Write("{0}{1}{2}\n", indent, indent, "password, The password to use when connecting to the server. Default is 'taosdata'.");
                    Console.Write("{0}{1}", indent, "-d");
                    Console.Write("{0}{1}{2}\n", indent, indent, "database, Destination database. Default is 'test'.");
                    Console.Write("{0}{1}", indent, "-a");
                    Console.Write("{0}{1}{2}\n", indent, indent, "replica, Set the replica parameters of the database, Default 1, min: 1, max: 5.");
                    Console.Write("{0}{1}", indent, "-m");
                    Console.Write("{0}{1}{2}\n", indent, indent, "table_prefix, Table prefix name. Default is 't'.");
                    Console.Write("{0}{1}", indent, "-s");
                    Console.Write("{0}{1}{2}\n", indent, indent, "sql file, The select sql file.");
                    Console.Write("{0}{1}", indent, "-M");
                    Console.Write("{0}{1}{2}\n", indent, indent, "stable, Use super table.");
                    Console.Write("{0}{1}", indent, "-o");
                    Console.Write("{0}{1}{2}\n", indent, indent, "outputfile, Direct output to the named file. Default is './output.txt'.");
                    Console.Write("{0}{1}", indent, "-q");
                    Console.Write("{0}{1}{2}\n", indent, indent, "query_mode, Query mode--0: SYNC, 1: ASYNC. Default is SYNC.");
                    Console.Write("{0}{1}", indent, "-b");
                    Console.Write("{0}{1}{2}\n", indent, indent, "type_of_cols, data_type of columns: 'INT', 'TINYINT', 'SMALLINT', 'BIGINT', 'FLOAT', 'DOUBLE', 'BINARY'. Default is 'INT'.");
                    Console.Write("{0}{1}", indent, "-w");
                    Console.Write("{0}{1}{2}\n", indent, indent, "length_of_binary, The length of data_type 'BINARY'. Only applicable when type of cols is 'BINARY'. Default is 8");
                    Console.Write("{0}{1}", indent, "-l");
                    Console.Write("{0}{1}{2}\n", indent, indent, "num_of_cols_per_record, The number of columns per record. Default is 3.");
                    Console.Write("{0}{1}", indent, "-T");
                    Console.Write("{0}{1}{2}\n", indent, indent, "num_of_threads, The number of threads. Default is 10.");
                    Console.Write("{0}{1}", indent, "-r");
                    Console.Write("{0}{1}{2}\n", indent, indent, "num_of_records_per_req, The number of records per request. Default is 1000.");
                    Console.Write("{0}{1}", indent, "-t");
                    Console.Write("{0}{1}{2}\n", indent, indent, "num_of_tables, The number of tables. Default is 1.");
                    Console.Write("{0}{1}", indent, "-n");
                    Console.Write("{0}{1}{2}\n", indent, indent, "num_of_records_per_table, The number of records per table. Default is 1.");
                    Console.Write("{0}{1}", indent, "-c");
                    Console.Write("{0}{1}{2}\n", indent, indent, "config_directory, Configuration directory. Default is '/etc/taos/'.");
                    Console.Write("{0}{1}", indent, "-x");
                    Console.Write("{0}{1}{2}\n", indent, indent, "flag, Insert only flag.");
                    Console.Write("{0}{1}", indent, "-O");
                    Console.Write("{0}{1}{2}\n", indent, indent, "order, Insert mode--0: In order, 1: Out of order. Default is in order.");
                    Console.Write("{0}{1}", indent, "-R");
                    Console.Write("{0}{1}{2}\n", indent, indent, "rate, Out of order data's rate--if order=1 Default 10, min: 0, max: 50.");
                    Console.Write("{0}{1}", indent, "-D");
                    Console.Write("{0}{1}{2}\n", indent, indent, "Delete data methods 0: don't delete, 1: delete by table, 2: delete by stable, 3: delete by database.");
                    Console.Write("{0}{1}", indent, "-v");
                    Console.Write("{0}{1}{2}\n", indent, indent, "Print verbose output");
                    Console.Write("{0}{1}", indent, "-y");
                    Console.Write("{0}{1}{2}\n", indent, indent, "Skip read key for continous test, default is not skip");

                    System.Environment.Exit(0);
                }
            }
        }

        public void ReadArgument(String[] argv)
        {
            host = this.GetArgumentAsString(argv, "-h", "127.0.0.1");
            port = (short)this.GetArgumentAsLong(argv, "-p", 0, 65535, 6030);
            user = this.GetArgumentAsString(argv, "-u", "root");
            password = this.GetArgumentAsString(argv, "-P", "taosdata");
            dbName = this.GetArgumentAsString(argv, "-d", "db");
            stableName = this.GetArgumentAsString(argv, "-s", "st");
            tablePrefix = this.GetArgumentAsString(argv, "-m", "t");
            isInsertOnly = this.GetArgumentAsFlag(argv, "-x");
            queryMode = (int)this.GetArgumentAsLong(argv, "-q", 0, 1, 0);
            numOfTables = this.GetArgumentAsLong(argv, "-t", 1, 1000000000, 1);
            batchRows = this.GetArgumentAsLong(argv, "-r", 1, 10000, 1000);
            recordsPerTable = this.GetArgumentAsLong(argv, "-n", 1, 100000000000, 1);
            recordsPerRequest = (int)this.GetArgumentAsLong(argv, "-r", 1, 10000, 1);
            colsPerRecord = (int)this.GetArgumentAsLong(argv, "-l", 1, 1024, 3);
            configDir = this.GetArgumentAsString(argv, "-c", "C:/TDengine/cfg");
            useStable = this.GetArgumentAsFlag(argv, "-M");

            replica = (short)this.GetArgumentAsLong(argv, "-a", 1, 5, 1);
            methodOfDelete = (short)this.GetArgumentAsLong(argv, "-D", 0, 3, 0);
            numOfThreads = (short)this.GetArgumentAsLong(argv, "-T", 1, 10000, 1);
            order = this.GetArgumentAsFlag(argv, "-O");
            rateOfOutorder = this.GetArgumentAsLong(argv, "-R", 0, 100, 0);

            skipReadKey = this.GetArgumentAsFlag(argv, "-y");
            verbose = this.GetArgumentAsFlag(argv, "-v");

            Console.Write("###################################################################\n");
            Console.Write("# Server IP:                         {0}\n", host);
            Console.Write("# User:                              {0}\n", user);
            Console.Write("# Password:                          {0}\n", password);
            Console.Write("# Number of Columns per record:      {0}\n", colsPerRecord);
            Console.Write("# Number of Threads:                 {0}\n", numOfThreads);
            Console.Write("# Number of Tables:                  {0}\n", numOfTables);
            Console.Write("# Number of Data per Table:          {0}\n", recordsPerTable);
            Console.Write("# Records/Request:                   {0}\n", recordsPerRequest);
            Console.Write("# Database name:                     {0}\n", dbName);
            Console.Write("# Replica:                           {0}\n", replica);
            Console.Write("# Use STable:                        {0}\n", useStable);
            Console.Write("# Table prefix:                      {0}\n", tablePrefix);
            Console.Write("# Data order:                        {0}\n", order);
            Console.Write("# Data out of order rate:            {0}\n", rateOfOutorder);
            Console.Write("# Delete method:                     {0}\n", methodOfDelete);
            Console.Write("# Query Mode:                        {0}\n", queryMode);
            Console.Write("# Insert Only:                       {0}\n", isInsertOnly);
            Console.Write("# Verbose output                     {0}\n", verbose);
            Console.Write("# Test time:                         {0}\n", DateTime.Now.ToString("h:mm:ss tt"));

            Console.Write("###################################################################\n");

            if (skipReadKey == false)
            {
                Console.Write("Press any key to continue..\n");
                Console.ReadKey();
            }
        }

        public bool GetArgumentAsFlag(String[] argv, String argName)
        {
            int argc = argv.Length;
            for (int i = 0; i < argc; ++i)
            {
                if (argName == argv[i])
                {
                    return true;
                }
            }
            return false;
        }

        public long GetArgumentAsLong(String[] argv, String argName, int minVal, long maxVal, int defaultValue)
        {
            int argc = argv.Length;
            for (int i = 0; i < argc; ++i)
            {
                if (argName != argv[i])
                {
                    continue;
                }
                if (i < argc - 1)
                {
                    String tmp = argv[i + 1];
                    if (tmp[0] == '-')
                    {
                        Console.WriteLine("option {0:G} requires an argument", tmp);
                        ExitProgram();
                    }

                    long tmpVal = Convert.ToInt64(tmp);
                    if (tmpVal < minVal || tmpVal > maxVal)
                    {
                        Console.WriteLine("option {0:G} should in range [{1:G}, {2:G}]", argName, minVal, maxVal);
                        ExitProgram();
                    }

                    return tmpVal;
                }
            }

            return defaultValue;
        }

        public String GetArgumentAsString(String[] argv, String argName, String defaultValue)
        {
            int argc = argv.Length;
            for (int i = 0; i < argc; ++i)
            {
                if (argName != argv[i])
                {
                    continue;
                }
                if (i < argc - 1)
                {
                    String tmp = argv[i + 1];
                    if (tmp[0] == '-')
                    {
                        Console.WriteLine("option {0:G} requires an argument", tmp);
                        ExitProgram();
                    }
                    return tmp;
                }
            }

            return defaultValue;
        }

        static void ExitProgram()
        {
            TDengine.Cleanup();
            System.Environment.Exit(0);
        }

        private void DebugPrintFormat(string format, params object[] parameters)
        {
            if (verbose == true)
            {
                Console.Write(format, parameters);
            }
        }

        private void DebugPrint(string str)
        {
            if (verbose == true)
            {
                Console.Write(str);
            }
        }

        public void InitTDengine()
        {
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_CONFIGDIR, this.configDir);
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            TDengine.Init();
            DebugPrint("TDengine Initialization finished\n");
        }

        public void ConnectTDengine()
        {
            string db = "";
            DebugPrintFormat("host:{0} user:{1}, pass:{2}; db:{3}, port:{4}\n",
                    this.host, this.user, this.password, db, this.port);
            this.conn = TDengine.Connect(this.host, this.user, this.password, db, this.port);
            if (this.conn == IntPtr.Zero)
            {
                Console.WriteLine("Connect to TDengine failed");
                ExitProgram();
            }
            else
            {
                DebugPrint("Connect to TDengine success\n");
            }
        }

        public void CreateTablesByThreads()
        {
            Thread[] threadArr = new Thread[numOfThreads];

            long quotition = numOfTables / numOfThreads;
            if (quotition < 1)
            {
                numOfThreads = numOfTables;
                quotition = 1;
            }

            long remainder = 0;
            if (numOfThreads != 0)
            {
                remainder = numOfTables % numOfThreads;
            }

            long last = 0;

            for (int i = 0; i < numOfThreads; i++)
            {
                CreateTableThread createTableThread = new CreateTableThread();
                createTableThread.id = i;
                createTableThread.verbose = verbose;
                createTableThread.dbName = this.dbName;
                createTableThread.tablePrefix = this.tablePrefix;
                createTableThread.useStable = useStable;
                if (useStable)
                {
                    createTableThread.stableName = stableName;
                }
                createTableThread.conn = conn;

                createTableThread.start = last;
                if (i < remainder)
                {
                    createTableThread.end = last + quotition;
                }
                else
                {
                    createTableThread.end = last + quotition - 1;
                }
                last = createTableThread.end + 1;

                threadArr[i] = new Thread(createTableThread.ThreadMain);
                threadArr[i].Start();
                threadArr[i].Join();
            }
        }

        public void dropDatabase()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("DROP DATABASE IF EXISTS ").Append(this.dbName);
            IntPtr res = TDengine.Query(this.conn, sql.ToString());
            if (res != IntPtr.Zero)
            {
                DebugPrint(sql.ToString() + " success\n");
            }
            else
            {
                Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(res));
                ExitProgram();
            }

        }

        public void CreateDb()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("CREATE DATABASE IF NOT EXISTS ").Append(this.dbName).Append(" replica ").Append(this.replica);
            IntPtr res = TDengine.Query(this.conn, sql.ToString());
            if (res != IntPtr.Zero)
            {
                DebugPrint(sql.ToString() + " success\n");
            }
            else
            {
                Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(res));
                ExitProgram();
            }
            TDengine.FreeResult(res);
        }

        public void CreateStable()
        {
            StringBuilder sql = new StringBuilder();

            sql.Clear();
            sql.Append("CREATE TABLE IF NOT EXISTS ").
                Append(this.dbName).Append(".").Append(this.stableName).
                Append("(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 bigint, v6 float, v7 double, v8 binary(10), v9 nchar(10)) tags(t1 int)");
            IntPtr res = TDengine.Query(this.conn, sql.ToString());
            if (res != IntPtr.Zero)
            {
                DebugPrint(sql.ToString() + " success\n");
            }
            else
            {
                Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(res));
                ExitProgram();
            }
            TDengine.FreeResult(res);
        }

        public void InsertByThreads()
        {
            Thread[] threadArr = new Thread[numOfThreads];

            long quotition = numOfTables / numOfThreads;
            if (quotition < 1)
            {
                numOfThreads = numOfTables;
                quotition = 1;
            }

            long remainder = 0;
            if (numOfThreads != 0)
            {
                remainder = numOfTables % numOfThreads;
            }

            long last = 0;

            for (int i = 0; i < numOfThreads; i++)
            {
                InsertDataThread insertThread = new InsertDataThread();
                insertThread.id = i;
                insertThread.recordsPerTable = recordsPerTable;
                insertThread.batchRows = batchRows;
                insertThread.numOfTables = numOfTables;
                insertThread.verbose = verbose;
                insertThread.dbName = this.dbName;
                insertThread.tablePrefix = this.tablePrefix;
                if (useStable)
                {
                    //                    insertThread.stableName = stableName;
                }
                insertThread.conn = conn;

                insertThread.start = last;
                if (i < remainder)
                {
                    insertThread.end = last + quotition;
                }
                else
                {
                    insertThread.end = last + quotition - 1;
                }
                last = insertThread.end + 1;

                threadArr[i] = new Thread(insertThread.ThreadMain);
                threadArr[i].Start();
                threadArr[i].Join();
            }
        }

        public void ExecuteQuery()
        {
            //            System.DateTime start = new System.DateTime();
            long queryRows = 0;

            for (int i = 0; i < 1/*this.numOfTables*/; ++i)
            {
                String sql = "select * from " + this.dbName + "." + tablePrefix + i;
                //                Console.WriteLine(sql);

                IntPtr res = TDengine.Query(conn, sql);
                if (res == IntPtr.Zero)
                {
                    Console.WriteLine(sql + " failure, reason: " + TDengine.Error(res));
                    ExitProgram();
                }

                int fieldCount = TDengine.FieldCount(res);
                //                Console.WriteLine("field count: " + fieldCount);

                List<TDengineMeta> metas = TDengine.FetchFields(res);
                for (int j = 0; j < metas.Count; j++)
                {
                    TDengineMeta meta = (TDengineMeta)metas[j];
                    //                    Console.WriteLine("index:" + j + ", type:" + meta.type + ", typename:" + meta.TypeName() + ", name:" + meta.name + ", size:" + meta.size);
                }

                IntPtr rowdata;
                StringBuilder builder = new StringBuilder();
                while ((rowdata = TDengine.FetchRows(res)) != IntPtr.Zero)
                {
                    queryRows++;
                    for (int fields = 0; fields < fieldCount; ++fields)
                    {
                        TDengineMeta meta = metas[fields];
                        int offset = IntPtr.Size * fields;
                        IntPtr data = Marshal.ReadIntPtr(rowdata, offset);

                        builder.Append("---");

                        if (data == IntPtr.Zero)
                        {
                            builder.Append("NULL");
                            continue;
                        }

                        switch ((TDengineDataType)meta.type)
                        {
                            case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                                bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                                builder.Append(v1);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                                byte v2 = Marshal.ReadByte(data);
                                builder.Append(v2);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                                short v3 = Marshal.ReadInt16(data);
                                builder.Append(v3);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_INT:
                                int v4 = Marshal.ReadInt32(data);
                                builder.Append(v4);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                                long v5 = Marshal.ReadInt64(data);
                                builder.Append(v5);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                                float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                                builder.Append(v6);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                                double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                                builder.Append(v7);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                                string v8 = Marshal.PtrToStringAnsi(data);
                                builder.Append(v8);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                                long v9 = Marshal.ReadInt64(data);
                                builder.Append(v9);
                                break;
                            case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                                string v10 = Marshal.PtrToStringAnsi(data);
                                builder.Append(v10);
                                break;
                        }
                    }
                    builder.Append("---");

                    if (queryRows <= 10)
                    {
                        Console.WriteLine(builder.ToString());
                    }
                    builder.Clear();
                }

                if (TDengine.ErrorNo(res) != 0)
                {
                    Console.Write("Query is not complete, Error {0:G}",
                            TDengine.ErrorNo(res), TDengine.Error(res));
                }

                TDengine.FreeResult(res);
            }
            /*
                        System.DateTime end = new System.DateTime();
                        TimeSpan ts = end - start;

                        Console.Write("Total {0:G} rows inserted, {1:G} rows query, time spend {2:G} seconds.\n"
                         , this.rowsInserted, queryRows, ts.TotalSeconds);
                         */
        }

        public void CloseConnection()
        {
            if (this.conn != IntPtr.Zero)
            {
                TDengine.Close(this.conn);
            }
        }

        // Main entry
        static void Main(string[] args)
        {
            PrintHelp(args);

            TDengineTest tester = new TDengineTest();
            tester.ReadArgument(args);

            tester.InitTDengine();
            tester.ConnectTDengine();

            if (tester.isInsertOnly == false)
            {
                tester.dropDatabase();
                tester.CreateDb();


                if (tester.useStable == true)
                {
                    tester.CreateStable();
                }

                tester.CreateTablesByThreads();
            }

            Stopwatch watch = Stopwatch.StartNew();
            tester.InsertByThreads();
            watch.Stop();
            double elapsedMs = watch.Elapsed.TotalMilliseconds;

            Console.WriteLine("Spent {0} seconds to insert {1} records with {2} record(s) per request: {3} records/second",
                    elapsedMs / 1000,
                tester.recordsPerTable * tester.numOfTables,
                tester.batchRows,
                (tester.recordsPerTable * tester.numOfTables * 1000) / elapsedMs);

            tester.ExecuteQuery();
            tester.CloseConnection();

            Console.WriteLine("End.");
        }

        public class InsertDataThread
        {
            public long id { set; get; }
            public long start { set; get; }
            public long end { set; get; }
            public string dbName { set; get; }
            public IntPtr conn { set; get; }
            public string tablePrefix { set; get; }
            //            public string stableName { set; get; }
            public long recordsPerTable { set; get; }
            public long batchRows { set; get; }
            public long numOfTables { set; get; }
            public bool verbose { set; get; }

            private void DebugPrintFormat(string format, params object[] parameters)
            {
                if (verbose == true)
                {
                    Console.Write(format, parameters);
                }
            }

            private void DebugPrint(string str)
            {
                if (verbose == true)
                {
                    Console.Write(str);
                }
            }

            public void ThreadMain()
            {
                DebugPrintFormat("InsertDataThread {0} from {1} to {2}\n", id, start, end);
                StringBuilder sql = new StringBuilder();

                DateTime now = DateTime.Now;
                int h = now.Hour;
                int m = now.Minute;
                int s = now.Second;

                long baseTimestamp = 1609430400000;    // 2021/01/01 0:0:0
                DebugPrintFormat("beginTime is {0} + {1}h:{2}m:{3}s\n", baseTimestamp, h, m, s);
                long beginTimestamp = baseTimestamp + ((h*60 + m) * 60 + s) * 1000;

                long rowsInserted = 0;

                //                System.DateTime startTime = new System.DateTime();
                long i = 0;
                while (i < recordsPerTable)
                {
                    for (long table = start; table <= end; ++table)
                    {
                        long inserted = i;

                        sql.Clear();
                        sql.Append("INSERT INTO ").
                            Append(this.dbName).Append(".").Append(this.tablePrefix).Append(table).
                            Append(" VALUES");
                        if (recordsPerTable < batchRows)
                        {
                            batchRows = recordsPerTable;
                        }
                        for (int batch = 0; batch < batchRows; ++batch)
                        {
                            sql.Append("(")
                                .Append(beginTimestamp + i + batch)
                                .Append(", 1, 2, 3,")
                                .Append(i + batch)
                                .Append(", 5, 6, 7, 'abc', 'def')");

                        }
                        IntPtr res = TDengine.Query(this.conn, sql.ToString());
                        if (res == IntPtr.Zero)
                        {
                            DebugPrint(sql.ToString() + " failure, reason: " + TDengine.Error(res) + "\n");
                        }

                        inserted += this.batchRows;

                        int affectRows = TDengine.AffectRows(res);
                        rowsInserted += affectRows;

                        TDengine.FreeResult(res);
                        if (table == end)
                        {
                            i = inserted;
                        }
                    }
                }

            }
        }

        public class CreateTableThread
        {
            public long id { set; get; }
            public long start { set; get; }
            public long end { set; get; }
            public string dbName { set; get; }
            public IntPtr conn { set; get; }
            public string tablePrefix { set; get; }
            public string stableName { set; get; }
            public bool verbose { set; get; }
            public bool useStable { set; get; }

            private void DebugPrintFormat(string format, params object[] parameters)
            {
                if (verbose == true)
                {
                    Console.Write(format, parameters);
                }
            }

            private void DebugPrint(string str)
            {
                if (verbose == true)
                {
                    Console.Write(str);
                }
            }

            public void ThreadMain()
            {
                DebugPrintFormat("CreateTable {0} from {1} to {2}\n", id, start, end);

                StringBuilder sql = new StringBuilder();

                for (long tableId = start; tableId <= end; tableId++)
                {
                    sql.Clear();
                    sql = sql.Append("CREATE TABLE IF NOT EXISTS ").
                        Append(this.dbName).Append(".").Append(this.tablePrefix).Append(tableId);
                    if (useStable == true)
                    {
                        sql = sql.Append(" USING ").Append(this.dbName).Append(".").Append(this.stableName).
                            Append(" TAGS(").Append(tableId).Append(")");
                    }
                    else
                    {
                        sql = sql.Append("(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 bigint, v6 float, v7 double, v8 binary(10), v9 nchar(10))");
                    }
                    IntPtr res = TDengine.Query(this.conn, sql.ToString());
                    if (res != IntPtr.Zero)
                    {
                        DebugPrint(sql.ToString() + " success\n");
                    }
                    else
                    {
                        DebugPrint(sql.ToString() + " failure, reason: " + TDengine.Error(res) + "\n");
                        ExitProgram();
                    }
                    TDengine.FreeResult(res);
                }

            }
        }
    }
}
