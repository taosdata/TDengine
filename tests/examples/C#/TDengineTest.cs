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

namespace TDengineDriver
{
  class TDengineTest
  {
    //connect parameters
    private string host;
    private string configDir;
    private string user;
    private string password;
    private int port = 0;

    //sql parameters
    private string dbName;
    private string stableName;
    private string tablePrefix;

    private bool isInsertData;
    private bool isQueryData;

    private long tableCount;
    private long totalRows;
    private long batchRows;
    private long beginTimestamp = 1551369600000L;

    private long conn = 0;
    private long rowsInserted = 0;

    static void Main(string[] args)
    {
      TDengineTest tester = new TDengineTest();
      tester.ReadArgument(args);

      Console.WriteLine("---------------------------------------------------------------");
      Console.WriteLine("Starting Testing...");
      Console.WriteLine("---------------------------------------------------------------");

      tester.InitTDengine();
      tester.ConnectTDengine();
      tester.CreateDbAndTable();
      tester.ExecuteInsert();
      tester.ExecuteQuery();
      tester.CloseConnection();

      Console.WriteLine("---------------------------------------------------------------");
      Console.WriteLine("Stop Testing...");
      Console.WriteLine("---------------------------------------------------------------");

    }

    public long GetArgumentAsLong(String[] argv, String argName, int minVal, int maxVal, int defaultValue)
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

    public void PrintHelp(String[] argv)
    {
      for (int i = 0; i < argv.Length; ++i)
      {
        if ("--help" == argv[i])
        {
          String indent = "    ";
          Console.WriteLine("taosTest is simple example to operate TDengine use C# Language.\n");
          Console.WriteLine("{0:G}{1:G}", indent, "-h");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "TDEngine server IP address to connect");
          Console.WriteLine("{0:G}{1:G}", indent, "-u");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "The TDEngine user name to use when connecting to the server, default is root");
          Console.WriteLine("{0:G}{1:G}", indent, "-p");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "The TDEngine user name to use when connecting to the server, default is taosdata");
          Console.WriteLine("{0:G}{1:G}", indent, "-d");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Database used to create table or import data, default is db");
          Console.WriteLine("{0:G}{1:G}", indent, "-s");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Super Tables used to create table, default is mt");
          Console.WriteLine("{0:G}{1:G}", indent, "-t");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Table prefixs, default is t");
          Console.WriteLine("{0:G}{1:G}", indent, "-w");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Whether to insert data");
          Console.WriteLine("{0:G}{1:G}", indent, "-r");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Whether to query data");
          Console.WriteLine("{0:G}{1:G}", indent, "-n");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "How many Tables to create, default is 10");
          Console.WriteLine("{0:G}{1:G}", indent, "-b");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "How many rows per insert batch, default is 10");
          Console.WriteLine("{0:G}{1:G}", indent, "-i");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "How many rows to insert, default is 100");
          Console.WriteLine("{0:G}{1:G}", indent, "-c");
          Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Configuration directory");

          ExitProgram();
        }
      }
    }

    public void ReadArgument(String[] argv)
    {
      PrintHelp(argv);
      host = this.GetArgumentAsString(argv, "-h", "192.168.100.128");
      user = this.GetArgumentAsString(argv, "-u", "root");
      password = this.GetArgumentAsString(argv, "-p", "taosdata");
      dbName = this.GetArgumentAsString(argv, "-d", "db");
      stableName = this.GetArgumentAsString(argv, "-s", "st");
      tablePrefix = this.GetArgumentAsString(argv, "-t", "t");
      isInsertData = this.GetArgumentAsLong(argv, "-w", 0, 1, 1) != 0;
      isQueryData = this.GetArgumentAsLong(argv, "-r", 0, 1, 1) != 0;
      tableCount = this.GetArgumentAsLong(argv, "-n", 1, 10000, 10);
      batchRows = this.GetArgumentAsLong(argv, "-b", 1, 1000, 500);
      totalRows = this.GetArgumentAsLong(argv, "-i", 1, 10000000, 10000);
      configDir = this.GetArgumentAsString(argv, "-c", "C:/TDengine/cfg");
    }

    public void InitTDengine()
    {
      TDengine.Options((int)TDengineInitOption.TDDB_OPTION_CONFIGDIR, this.configDir);
      TDengine.Options((int)TDengineInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
      TDengine.Init();
      Console.WriteLine("TDengine Initialization finished");
    }

    public void ConnectTDengine()
    {
      string db = "";
      this.conn = TDengine.Connect(this.host, this.user, this.password, db, this.port);
      if (this.conn == 0)
      {
        Console.WriteLine("Connect to TDengine failed");
        ExitProgram();
      }
      else
      {
        Console.WriteLine("Connect to TDengine success");
      }
    }

    public void CreateDbAndTable()
    {
      if (!this.isInsertData)
      {
        return;
      }

      StringBuilder sql = new StringBuilder();
      sql.Append("create database if not exists ").Append(this.dbName);
      int code = TDengine.Query(this.conn, sql.ToString());
      if (code == TDengine.TSDB_CODE_SUCCESS)
      {
        Console.WriteLine(sql.ToString() + " success");
      }
      else
      {
        Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(conn));
        ExitProgram();
      }

      sql.Clear();
      sql.Append("use ").Append(this.dbName);
      code = TDengine.Query(this.conn, sql.ToString());
      if (code == TDengine.TSDB_CODE_SUCCESS)
      {
        Console.WriteLine(sql.ToString() + " success");
      }
      else
      {
        Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(this.conn));
        ExitProgram();
      }

      sql.Clear();
      sql.Append("create table if not exists ").Append(this.stableName).Append("(ts timestamp, v1 int) tags(t1 int)");
      code = TDengine.Query(this.conn, sql.ToString());
      if (code == TDengine.TSDB_CODE_SUCCESS)
      {
        Console.WriteLine(sql.ToString() + " success");
      }
      else
      {
        Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(this.conn));
        ExitProgram();
      }

      for (int i = 0; i < this.tableCount; i++)
      {
        sql.Clear();
        sql = sql.Append("create table if not exists ").Append(this.tablePrefix).Append(i)
          .Append(" using ").Append(this.stableName).Append(" tags(").Append(i).Append(")");
        code = TDengine.Query(this.conn, sql.ToString());
        if (code == TDengine.TSDB_CODE_SUCCESS)
        {
          Console.WriteLine(sql.ToString() + " success");
        }
        else
        {
          Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(this.conn));
          ExitProgram();
        }
      }

      Console.WriteLine("create db and table success");
    }

    public void ExecuteInsert()
    {
      if (!this.isInsertData)
      {
        return;
      }

      System.DateTime start = new System.DateTime();
      long loopCount = this.totalRows / this.batchRows;

      for (int table = 0; table < this.tableCount; ++table)
      {
        for (long loop = 0; loop < loopCount; loop++)
        {
          StringBuilder sql = new StringBuilder();
          sql.Append("insert into ").Append(this.tablePrefix).Append(table).Append(" values");
          for (int batch = 0; batch < this.batchRows; ++batch)
          {
            long rows = loop * this.batchRows + batch;
            sql.Append("(").Append(this.beginTimestamp + rows).Append(",").Append(rows).Append(")");
          }
          int code = TDengine.Query(conn, sql.ToString());
          if (code != TDengine.TSDB_CODE_SUCCESS)
          {
            Console.WriteLine(sql.ToString() + " failure, reason: " + TDengine.Error(conn));
          }

          int affectRows = TDengine.AffectRows(conn);
          this.rowsInserted += affectRows;
        }
      }

      System.DateTime end = new System.DateTime();
      TimeSpan ts = end - start;

      Console.Write("Total {0:G} rows inserted, {1:G} rows failed, time spend {2:G} seconds.\n"
        , this.rowsInserted, this.totalRows * this.tableCount - this.rowsInserted, ts.TotalSeconds);
    }

    public void ExecuteQuery()
    {
      if (!this.isQueryData)
      {
        return;
      }

      System.DateTime start = new System.DateTime();
      long queryRows = 0;

      for (int i = 0; i < this.tableCount; ++i)
      {
        String sql = "select * from " + this.dbName + "." + tablePrefix + i;
        Console.WriteLine(sql);

        int code = TDengine.Query(conn, sql);
        if (code != TDengine.TSDB_CODE_SUCCESS)
        {
          Console.WriteLine(sql + " failure, reason: " + TDengine.Error(conn));
          ExitProgram();
        }

        int fieldCount = TDengine.FieldCount(conn);
        //Console.WriteLine("field count: " + fieldCount);

        List<TDengineMeta> metas = TDengine.FetchFields(conn);
        for (int j = 0; j < metas.Count; j++)
        {
          TDengineMeta meta = (TDengineMeta)metas[j];
          //Console.WriteLine("index:" + j + ", type:" + meta.type + ", typename:" + meta.TypeName() + ", name:" + meta.name + ", size:" + meta.size);
        }

        long result = TDengine.UseResult(conn);
        if (result == 0)
        {
          Console.WriteLine(sql + " result set is null");
          return;
        }

        IntPtr rowdata;
        while ((rowdata = TDengine.FetchRows(result)) != IntPtr.Zero)
        {
          queryRows++;
          for (int fields = 0; fields < fieldCount; ++fields)
          {
            TDengineMeta meta = metas[fields];
            int offset = 8 * fields;
            IntPtr data = Marshal.ReadIntPtr(rowdata, offset);

            //Console.Write("---");

            if (data == IntPtr.Zero)
            {
              //Console.Write("NULL");
              continue;
            }

            switch ((TDengineDataType)meta.type)
            {
              case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                //Console.Write(v1);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                byte v2 = Marshal.ReadByte(data);
                //Console.Write(v2);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                short v3 = Marshal.ReadInt16(data);
                //Console.Write(v3);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_INT:
                int v4 = Marshal.ReadInt32(data);
                //Console.Write(v4);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                long v5 = Marshal.ReadInt64(data);
                //Console.Write(v5);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                //Console.Write(v6);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                //Console.Write(v7);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                string v8 = Marshal.PtrToStringAnsi(data);
                //Console.Write(v8);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                long v9 = Marshal.ReadInt64(data);
                //Console.Write(v9);
                break;
              case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                string v10 = Marshal.PtrToStringAnsi(data);
                //Console.Write(v10);
                break;
            }
          }
          //Console.WriteLine("---");
        }

        if (TDengine.ErrorNo(conn) != 0)
        {
          Console.Write("Query is not complete， Error {0:G}", TDengine.ErrorNo(conn), TDengine.Error(conn));
        }

        TDengine.FreeResult(result);
      }

      System.DateTime end = new System.DateTime();
      TimeSpan ts = end - start;

      Console.Write("Total {0:G} rows inserted, {1:G} rows query, time spend {2:G} seconds.\n"
       , this.rowsInserted, queryRows, ts.TotalSeconds);
    }

    public void CloseConnection()
    {
      if (conn != 0)
      {
        TDengine.Close(conn);
      }
    }

    static void ExitProgram()
    {
      System.Environment.Exit(0);
    }
  }
}
