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
    class TDengineNanoTest
    {
        //connect parameters
        private string host="localhost";
        private string configDir="/etc/taos";
        private string user="root";
        private string password="taosdata";
        private short port = 0;

        private IntPtr conn = IntPtr.Zero;

        static void Main(string[] args)
        {
            TDengineNanoTest tester = new TDengineNanoTest();
            //tester.ReadArgument(args);

            tester.InitTDengine();
            tester.ConnectTDengine();
			tester.execute("reset query cache");
			tester.execute("drop database if exists db");
			tester.execute("create database db precision 'ns'");
			tester.executeQuery("show databases;");
			//tester.checkData(0,16,"ns");
			tester.execute("use db");
			
			Console.WriteLine("testing nanosecond support in 1st timestamp");
            tester.execute("create table tb (ts timestamp, speed int)");
			tester.execute("insert into tb values('2021-06-10 0:00:00.100000001', 1);");
			tester.execute("insert into tb values(1623254400150000000, 2);");
			tester.execute("import into tb values(1623254400300000000, 3);");
			tester.execute("import into tb values(1623254400299999999, 4);");
			tester.execute("insert into tb values(1623254400300000001, 5);");
			tester.execute("insert into tb values(1623254400999999999, 7);");
            tester.executeQuery("select * from tb;");
            
            Console.WriteLine("expect data is ");

            tester.executeQuery("select * from tb;");

            tester.executeQuery("select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400100000002;");
            Console.WriteLine("expected is : 1 " );
            tester.executeQuery("select count(*) from tb where ts > '2021-06-10 0:00:00.100000001' and ts < '2021-06-10 0:00:00.160000000';");
            Console.WriteLine("expected is : 1 " );

            tester.executeQuery("select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400150000000;");
            Console.WriteLine("expected is : 1 " );
            tester.executeQuery("select count(*) from tb where ts > '2021-06-10 0:00:00.100000000' and ts < '2021-06-10 0:00:00.150000000';");
            Console.WriteLine("expected is : 1 " );

            tester.executeQuery("select count(*) from tb where ts > 1623254400400000000;");
            Console.WriteLine("expected is : 1 " );
            tester.executeQuery("select count(*) from tb where ts < '2021-06-10 00:00:00.400000000';");
            Console.WriteLine("expected is : 5 " );

            tester.executeQuery("select count(*) from tb where ts > now + 400000000b;");
            Console.WriteLine("expected is : 0 " );

            tester.executeQuery("select count(*) from tb where ts >= '2021-06-10 0:00:00.100000001';");
            Console.WriteLine("expected is : 6 " );

            tester.executeQuery("select count(*) from tb where ts <= 1623254400300000000;");
            Console.WriteLine("expected is : 4 " );

            tester.executeQuery("select count(*) from tb where ts = '2021-06-10 0:00:00.000000000';");
            Console.WriteLine("expected is : 0 " );

            tester.executeQuery("select count(*) from tb where ts = 1623254400150000000;");
            Console.WriteLine("expected is : 1 " );

            tester.executeQuery("select count(*) from tb where ts = '2021-06-10 0:00:00.100000001';");
            Console.WriteLine("expected is : 1 " );

            tester.executeQuery("select count(*) from tb where ts between 1623254400000000000 and 1623254400400000000;");
            Console.WriteLine("expected is : 5 " );

            tester.executeQuery("select count(*) from tb where ts between '2021-06-10 0:00:00.299999999' and '2021-06-10 0:00:00.300000001';");
            Console.WriteLine("expected is : 3 " );

            tester.executeQuery("select avg(speed) from tb interval(5000000000b);");
            Console.WriteLine("expected is : 1 " );

            tester.executeQuery("select avg(speed) from tb interval(100000000b)");
            Console.WriteLine("expected is : 4 " );

            // tdSql.error("select avg(speed) from tb interval(1b);")
            // tdSql.error("select avg(speed) from tb interval(999b);")

            tester.executeQuery("select avg(speed) from tb interval(1000b);");
            Console.WriteLine("expected is : 5 rows " );

            tester.executeQuery("select avg(speed) from tb interval(1u);");
            Console.WriteLine("expected is : 5 rows " );

            tester.executeQuery("select avg(speed) from tb interval(100000000b) sliding (100000000b);");
            Console.WriteLine("expected is : 4 rows " );

            tester.executeQuery("select last(*) from tb");
            Console.WriteLine("expected is :1623254400999999999 " );

            // tdSql.checkData(0,0, "2021-06-10 0:00:00.999999999")
            // tdSql.checkData(0,0, 1623254400999999999)

            tester.executeQuery("select first(*) from tb");
            Console.WriteLine("expected is : 1623254400100000001" );
            // tdSql.checkData(0,0, 1623254400100000001);
            // tdSql.checkData(0,0, "2021-06-10 0:00:00.100000001");

            tester.execute("insert into tb values(now + 500000000b, 6);");
            tester.executeQuery("select * from tb;");
            // tdSql.checkRows(7);

            tester.execute("create table tb2 (ts timestamp, speed int, ts2 timestamp);");
            tester.execute("insert into tb2 values('2021-06-10 0:00:00.100000001', 1, '2021-06-11 0:00:00.100000001');");
            tester.execute("insert into tb2 values(1623254400150000000, 2, 1623340800150000000);");
            tester.execute("import into tb2 values(1623254400300000000, 3, 1623340800300000000);");
            tester.execute("import into tb2 values(1623254400299999999, 4, 1623340800299999999);");
            tester.execute("insert into tb2 values(1623254400300000001, 5, 1623340800300000001);");
            tester.execute("insert into tb2 values(1623254400999999999, 7, 1623513600999999999);");

            tester.executeQuery("select * from tb2;");
            // tdSql.checkData(0,0,"2021-06-10 0:00:00.100000001");
            // tdSql.checkData(1,0,"2021-06-10 0:00:00.150000000");
            // tdSql.checkData(2,1,4);
            // tdSql.checkData(3,1,3);
            // tdSql.checkData(4,2,"2021-06-11 00:00:00.300000001");
            // tdSql.checkData(5,2,"2021-06-13 00:00:00.999999999");
            // tdSql.checkRows(6);
            tester.executeQuery("select count(*) from tb2 where ts2 > 1623340800000000000 and ts2 < 1623340800150000000;");
            Console.WriteLine("expected is : 1 " );
           // tdSql.checkData(0,0,1);
            
            tester.executeQuery("select count(*) from tb2 where ts2 > '2021-06-11 0:00:00.100000000' and ts2 < '2021-06-11 0:00:00.100000002';");
            Console.WriteLine("expected is : 1 " );
            // tdSql.checkData(0,0,1);

            tester.executeQuery("select count(*) from tb2 where ts2 > 1623340800500000000;");
            Console.WriteLine("expected is : 1 " );
            // tdSql.checkData(0,0,1);
            tester.executeQuery("select count(*) from tb2 where ts2 < '2021-06-11 0:00:00.400000000';");
            Console.WriteLine("expected is : 5 " );
            // tdSql.checkData(0,0,5);

            tester.executeQuery("select count(*) from tb2 where ts2 > now + 400000000b;");
            Console.WriteLine("expected is : 0 " );
            // tdSql.checkRows(0);

            tester.executeQuery("select count(*) from tb2 where ts2 >= '2021-06-11 0:00:00.100000001';");
            Console.WriteLine("expected is : 6 " );
            // tdSql.checkData(0,0,6);

            tester.executeQuery("select count(*) from tb2 where ts2 <= 1623340800400000000;");
            Console.WriteLine("expected is : 5 " );
            // tdSql.checkData(0,0,5);

            tester.executeQuery("select count(*) from tb2 where ts2 = '2021-06-11 0:00:00.000000000';");
            Console.WriteLine("expected is : 0 " );
            // tdSql.checkRows(0);

            tester.executeQuery("select count(*) from tb2 where ts2 = '2021-06-11 0:00:00.300000001';");
            Console.WriteLine("expected is : 1 " );
            // tdSql.checkData(0,0,1);

            tester.executeQuery("select count(*) from tb2 where ts2 = 1623340800300000001;");
            Console.WriteLine("expected is : 1 " );
            // tdSql.checkData(0,0,1);

            tester.executeQuery("select count(*) from tb2 where ts2 between 1623340800000000000 and 1623340800450000000;");
            Console.WriteLine("expected is : 5 " );
            // tdSql.checkData(0,0,5);

            tester.executeQuery("select count(*) from tb2 where ts2 between '2021-06-11 0:00:00.299999999' and '2021-06-11 0:00:00.300000001';");
            Console.WriteLine("expected is : 3 " );
            // tdSql.checkData(0,0,3);

            tester.executeQuery("select count(*) from tb2 where ts2 <> 1623513600999999999;");
            Console.WriteLine("expected is : 5 " );
            // tdSql.checkData(0,0,5);

            tester.executeQuery("select count(*) from tb2 where ts2 <> '2021-06-11 0:00:00.100000001';");
            Console.WriteLine("expected is : 5 " );
            // tdSql.checkData(0,0,5);

            tester.executeQuery("select count(*) from tb2 where ts2 <> '2021-06-11 0:00:00.100000000';");
            Console.WriteLine("expected is : 6 " );
            // tdSql.checkData(0,0,6);

            tester.executeQuery("select count(*) from tb2 where ts2 != 1623513600999999999;");
            Console.WriteLine("expected is : 5 " );
            // tdSql.checkData(0,0,5);

            tester.executeQuery("select count(*) from tb2 where ts2 != '2021-06-11 0:00:00.100000001';");
            Console.WriteLine("expected is : 5 " );
            // tdSql.checkData(0,0,5);

            tester.executeQuery("select count(*) from tb2 where ts2 != '2021-06-11 0:00:00.100000000';");
            Console.WriteLine("expected is : 6 " );
            // tdSql.checkData(0,0,6);

            tester.execute("insert into tb2 values(now + 500000000b, 6, now +2d);");
            tester.executeQuery("select * from tb2;");
            Console.WriteLine("expected is : 7 rows" );
            // tdSql.checkRows(7);

            // tdLog.debug("testing ill nanosecond format handling");
            tester.execute("create table tb3 (ts timestamp, speed int);");
            // tdSql.error("insert into tb3 values(16232544001500000, 2);");
            tester.execute("insert into tb3 values('2021-06-10 0:00:00.123456', 2);");
            tester.executeQuery("select * from tb3 where ts = '2021-06-10 0:00:00.123456000';");
            // tdSql.checkRows(1);
            Console.WriteLine("expected is : 1 rows " );

            tester.execute("insert into tb3 values('2021-06-10 0:00:00.123456789000', 2);");
            tester.executeQuery("select * from tb3 where ts = '2021-06-10 0:00:00.123456789';");
            // tdSql.checkRows(1);
            Console.WriteLine("expected is : 1 rows " );

            // check timezone support 
            Console.WriteLine("nsdb" );
            tester.execute("drop database if exists nsdb;");
            tester.execute("create database nsdb precision 'ns';");
            tester.execute("use nsdb;");
            tester.execute("create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);");
            tester.execute("insert into tb1 using st tags('2021-06-10 0:00:00.123456789' , 1 ) values('2021-06-10T0:00:00.123456789+07:00' , 1.0);" );
            tester.executeQuery("select first(*) from tb1;");
            Console.WriteLine("expected is : 1623258000123456789 " );
            // tdSql.checkData(0,0,1623258000123456789);
           
            
            Console.WriteLine("usdb" );
            tester.execute("drop database if exists usdb;");
            tester.execute("create database usdb precision 'us';");
            tester.execute("use usdb;");
            tester.execute("create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);");
            tester.execute("insert into tb1 using st tags('2021-06-10 0:00:00.123456' , 1 ) values('2021-06-10T0:00:00.123456+07:00' , 1.0);" );
            tester.executeQuery("select first(*) from tb1;");

            Console.WriteLine("expected is : 1623258000123456 " );

            Console.WriteLine("msdb" );
            tester.execute("drop database if exists msdb;");
            tester.execute("create database msdb precision 'ms';");
            tester.execute("use msdb;");
            tester.execute("create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);");
            tester.execute("insert into tb1 using st tags('2021-06-10 0:00:00.123' , 1 ) values('2021-06-10T0:00:00.123+07:00' , 1.0);" );
            tester.executeQuery("select first(*) from tb1;");
            Console.WriteLine("expected is : 1623258000123 " );
            
            tester.CloseConnection();
            tester.cleanup();
        }
		
		public void InitTDengine()
        {
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_CONFIGDIR, this.configDir);
            TDengine.Options((int)TDengineInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            Console.WriteLine("init...");
            TDengine.Init();
            Console.WriteLine("get connection starting...");
        }
		
		public void ConnectTDengine()
        {
            string db = "";
            this.conn = TDengine.Connect(this.host, this.user, this.password, db, this.port);
            if (this.conn == IntPtr.Zero)
            {
                Console.WriteLine("connection failed: " + this.host);
                ExitProgram();
            }
            else
            {
                Console.WriteLine("[ OK ] Connection established.");
            }
        }
		
        //EXECUTE SQL
		public void execute(string sql)
        {
            DateTime dt1 = DateTime.Now;
            IntPtr res = TDengine.Query(this.conn, sql.ToString());
            DateTime dt2 = DateTime.Now;

            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
              Console.Write(sql.ToString() + " failure, ");
              if (res != IntPtr.Zero) {
                Console.Write("reason: " + TDengine.Error(res));
              }
              Console.WriteLine("");
              ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");
            }
            TDengine.FreeResult(res);
        }
		//EXECUTE QUERY 
		public void executeQuery(string sql)
        {

            DateTime dt1 = DateTime.Now;
            long queryRows = 0;
            IntPtr res = TDengine.Query(conn, sql);
            getPrecision(res);
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
              Console.Write(sql.ToString() + " failure, ");
              if (res != IntPtr.Zero) {
                Console.Write("reason: " + TDengine.Error(res));
              }
              Console.WriteLine("");
              ExitProgram();
            }
            DateTime dt2 = DateTime.Now;
            TimeSpan span = dt2 - dt1;
            Console.WriteLine("[OK] time cost: " + span.ToString() + "ms, execute statement ====> " + sql.ToString());
            int fieldCount = TDengine.FieldCount(res);

            List<TDengineMeta> metas = TDengine.FetchFields(res);
            for (int j = 0; j < metas.Count; j++)
            {
                TDengineMeta meta = (TDengineMeta)metas[j];
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
                Console.Write("Query is not complete, Error {0:G}", TDengine.ErrorNo(res), TDengine.Error(res));
            }
            Console.WriteLine("");

            TDengine.FreeResult(res);

        }
	
		public void CloseConnection()
        {
            if (this.conn != IntPtr.Zero)
            {
                TDengine.Close(this.conn);
                Console.WriteLine("connection closed.");
            }
        }

        static void ExitProgram()
        {
            System.Environment.Exit(0);
        }

        public void cleanup()
        {
            TDengine.Cleanup();
            Console.WriteLine("clean up...");
            System.Environment.Exit(0);
        }
		
		// method to get db precision
        public void getPrecision(IntPtr res)
        {
            int psc=TDengine.ResultPrecision(res);
            switch(psc)
            {
                case 0:
                    Console.WriteLine("db：[{0:G}]'s precision is {1:G} millisecond");
                    break;
                case 1:
                    Console.WriteLine("db：[{0:G}]'s precision is {1:G} microsecond");
                    break;
                case 2:
                    Console.WriteLine("db：[{0:G}]'s precision is {1:G} nanosecond");
                    break;
            }
        }		
	}
}

