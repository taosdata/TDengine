using System;
using TDengineDriver;
using Sample.UtilsTools;
using System.Runtime.InteropServices;
using System.Threading;
using System.Collections.Generic;
using System.Text;

namespace Example
{
    public class StreamSample
    {

        public void RunStreamOption1(IntPtr conn, string table)
        {

            PrepareData(conn, table);
            StreamOpenCallback streamOpenCallback = new StreamOpenCallback(StreamCallback);
            IntPtr stream = TDengine.OpenStream(conn, $"select count(*) from {table} interval(1m) sliding(30s)", streamOpenCallback, 0, IntPtr.Zero, null);
            if (stream == IntPtr.Zero)
            {
                throw new Exception("OPenStream failed");
            }
            else
            {
                Thread.Sleep(100000);
                AddNewData(conn, table, 5,true);
                Thread.Sleep(100000);

                TDengine.CloseStream(stream);
                Console.WriteLine("stream done");

            }
        }


        public void StreamCallback(IntPtr param, IntPtr taosRes, IntPtr taosRow)
        {

            if (taosRes == IntPtr.Zero || taosRow == IntPtr.Zero)
            {
                return;
            }
            else
            {
                var rowData = new List<Object>();
                rowData = UtilsTools.FetchRow(taosRow, taosRes);
                int count = 0;
                rowData.ForEach((item) =>
                {

                    Console.Write("{0} \t|\t", item.ToString());
                    count++;
                    if (count % rowData.Count == 0)
                    {
                        Console.WriteLine("");
                    }
                });
            }
        }

        public void PrepareData(IntPtr conn, string tableName)
        {
            string createTable = $"create table if not exists {tableName} (ts timestamp,i8 tinyint,i16 smallint,i32 int,i64 bigint);";
            UtilsTools.ExecuteUpdate(conn, createTable);
            AddNewData(conn, tableName, 5);
        }

        public void AddNewData(IntPtr conn, string tableName, int numRows,bool interval = false)
        {
            long ts = 1646150410100;
            Random rs = new Random();
            StringBuilder insert = new StringBuilder();

            Random rd = new Random();
            for (int i = 0; i < numRows; i++)
            {
                insert.Append("insert into ");
                insert.Append(tableName);
                insert.Append(" values ");
                insert.Append('(');
                insert.Append(ts);
                insert.Append(',');
                insert.Append(rs.Next(sbyte.MinValue+1, sbyte.MaxValue));
                insert.Append(',');
                insert.Append(rs.Next(short.MinValue+1, short.MaxValue));
                insert.Append(',');
                insert.Append(rs.Next(int.MinValue+1, int.MaxValue));
                insert.Append(',');
                insert.Append(rs.Next(int.MinValue+1, int.MaxValue));
                insert.Append(')');
                UtilsTools.ExecuteUpdate(conn, insert.ToString());
                insert.Clear();
                ts += rd.Next(10000, 100000);
                if( interval)
                {
                    Thread.Sleep(rs.Next(100,300) * i);
                }
                else
                {
                    continue;
                }
            }
        }

    }
}