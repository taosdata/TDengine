using System;
using TDengineDriver;
using Sample.UtilsTools;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Threading;

namespace Example
{

    public class SubscribeSample
    {
        long ts = 1646150410000;
        public void RunSubscribeWithCallback(IntPtr conn, string table)
        {
            PrepareData(conn, table);
            string topic = $"{table}_topic";
            string sql = $"select * from {table}";
            SubscribeCallback subscribeCallback = new SubscribeCallback(SubCallback);
            
            // Subscribe from earliest timestamp in the table.
            IntPtr subscribe = TDengine.Subscribe(conn, true, topic, sql, subscribeCallback, IntPtr.Zero, 1000);

            // Add new data.
            for (int i = 0; i < 4; i++)
            {
                InsertData(conn, table);
            }
            Console.WriteLine("Unsubscribe and keep the subscribe progress ");
            TDengine.Unsubscribe(subscribe, true);
            
            Console.WriteLine("Subscribe from last subscribe progress");
            subscribe = TDengine.Subscribe(conn, false, topic, sql, subscribeCallback, IntPtr.Zero, 1000);
            for (int i = 0; i < 4; i++)
            {
                InsertData(conn, table);
            }
            Console.WriteLine("Unsubscribe and remove the subscribe progress ");
            TDengine.Unsubscribe(subscribe, false);
        }

        public void RunSubscribeWithoutCallback(IntPtr conn, string table)
        {
            
            PrepareData(conn, table);
            string topic = $"{table}_topic";
            string sql = $"select * from {table}";
            IntPtr subscribe = TDengine.Subscribe(conn, true, topic, sql, null, IntPtr.Zero, 1000);
            Console.WriteLine("consume from begin");
            //Don't release this TAO_RES and end of the subscribe application,other wise will lead crash.
            IntPtr taosRes = TDengine.Consume(subscribe);
            UtilsTools.DisplayRes(taosRes);
            for (int i = 0; i < 3; i++)
            {
                InsertData(conn, table);
            }
            Console.WriteLine("consume new data");
            taosRes = TDengine.Consume(subscribe);
            UtilsTools.DisplayRes(taosRes);
            Console.WriteLine("Unsubscribe and keep progress");
            TDengine.Unsubscribe(subscribe, true);

            // Subscribe from last subscribe progress. 
            subscribe = TDengine.Subscribe(conn, false, topic, sql, null, IntPtr.Zero, 1000);           
            for (int i = 0; i < 3; i++)
            {
                InsertData(conn, table);
                Console.WriteLine($"Consume {i+1} time");
                taosRes = TDengine.Consume(subscribe);
                // The interval between two consume should greater than "interval" pass in Subscribe().
                // Otherwise consume will be blocked.
                Thread.Sleep(1000);
                UtilsTools.DisplayRes(taosRes);
            }
            TDengine.Unsubscribe(subscribe, false);
            TDengine.FreeResult(taosRes);
        }

        public void SubCallback(IntPtr subscribe, IntPtr taosRes, IntPtr param, int code)
        {
            if (code == 0 && taosRes != IntPtr.Zero)
            {
                // cannot free taosRes using taosRes, otherwise will cause crash.
                Console.WriteLine($"Display taosRes in subscribe callback");
                UtilsTools.DisplayRes(taosRes);
            }
            else
            {
                Console.WriteLine($"async query data failed, failed code {code}");
            }

        }
        public void PrepareData(IntPtr conn, string tableName)
        {
            string createTable = $"create table if not exists {tableName} (ts timestamp,i8 tinyint,i16 smallint,i32 int,i64 bigint);";
            string insert1 = $"insert into {tableName} values({ts},1,2,3,4)";

            UtilsTools.ExecuteUpdate(conn, createTable);
            UtilsTools.ExecuteUpdate(conn, insert1);
        }

        public void InsertData(IntPtr conn, string tableName)
        {
            ts = ts + 100;
            string insert1 = $"insert into {tableName} values({ts},1,2,3,4)";
            ts = ts + 100;
            string insert2 = $"insert into {tableName} values({ts},-1,-2,-3,-4)";

            UtilsTools.ExecuteUpdate(conn, insert1);
            UtilsTools.ExecuteUpdate(conn, insert2);
            Thread.Sleep(500);
        }

    }
}
