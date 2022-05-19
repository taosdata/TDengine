using System;
using Test.UtilsTools;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using Test.UtilsTools.ResultSet;

namespace insertCn
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            IntPtr conn =  UtilsTools.TDConnection();
            string dbName = "insert_cn_to_nchar_sample_dotnet";
            string  createDB = $"create database if not exists {dbName};";
            string  dropDB = $"drop database if exists {dbName};";
            string useDB = $"use {dbName};";
            string table = "t1";
            string stable = "stb";
            UtilsTools.ExecuteUpdate(conn,createDB);
            UtilsTools.ExecuteUpdate(conn,useDB);

            Console.WriteLine("=====================ntable====================");
            TestNtable(conn,table);
            Console.WriteLine("=====================stable====================");
            TestStable(conn,stable);
            
            UtilsTools.ExecuteUpdate(conn,dropDB);
            UtilsTools.CloseConnection(conn);

        }

        static void TestStable(IntPtr conn,string stable)
        {
           string createSql = $"create table if not exists {stable} (ts timestamp," +
            $"v4 int," +
            $"blob nchar(200)," +
            $"locate nchar(200)," +
            $"country binary(200)," +
            $"city binary(50)" +
            $")tags(" +
            $"id int," +
            $"name nchar(50)," +
            $"addr nchar(200)," +
            $"en_name binary(200));";

            String dropTb = "drop table if exists " + stable;
            String  table = stable + "_subtable_1";
            var colData = new List<Object>{1637064040000,1,"涛思数据","保利广场","Beijing","China",
            1637064041000,2,"涛思数据taosdata","保利广场baoli","Beijing","China",
            1637064042000,3,"TDengine涛思数据","time广场","NewYork","US",
            1637064043000,4,"4涛思数据","4广场南部","London","UK",
            1637064044000,5,"涛思数据5","!广场路中部123","Tokyo","JP",
            1637064045000,6,"taos涛思数据6","青年广场123号！","Washin","DC",
            1637064046000,7,"7涛思数据taos","asdf#壮年广场%#endregion","NewYork","US",
            1637064047000,8,"8&涛思数据taos","incluse阿斯顿发","NewYork","US",
            1637064048000,9,"&涛思数据taos9","123黑化肥werq会挥……&¥%发！afsdfa","NewYork","US",
            };
            var tagData = new List<Object>{1,"涛思数据","中国北方&南方长江黄河！49wq","tdengine"};
            string insertSql = UtilsTools.ConstructInsertSql(table, stable, colData, tagData, 9);
            string selectSql = $"select * from {stable};";
            List<Object> insertData = UtilsTools.CombineColAndTagData(colData,tagData,9);
            
            UtilsTools.ExecuteUpdate(conn,dropTb);
            UtilsTools.ExecuteUpdate(conn,createSql);
            UtilsTools.ExecuteUpdate(conn,insertSql);
            IntPtr res = UtilsTools.ExecuteQuery(conn,selectSql);

            ResultSet resultSet = new ResultSet(res);
            List<Object> queryResult = resultSet.GetResultData();
            
            //display
            int fieldsCount = resultSet.GetFieldsNum(); 
            for(int i = 0 ; i<queryResult.Count;i++){
                Console.Write(queryResult[i].ToString());
                Console.Write("\t");
                if((i+1)%fieldsCount == 0)
                {
                   Console.WriteLine(""); 
                }                
            }

            if(insertData.Count == queryResult.Count)
            {
                Console.WriteLine("insert data count = retrieve data count");
                for(int i = 0 ; i<queryResult.Count;i++)
                {
                    if(!queryResult[i].Equals(insertData[i]))
                    {
                        Console.Write("[Unequal Data]");
                        Console.WriteLine("InsertData:{0},QueryData:{1}",queryResult[i],insertData[i]);
                    }
                }
            }
        }

        static void TestNtable(IntPtr conn,string tableName)
        {
            var colData = new List<Object>{1637064040000,1,"涛思数据","保利广场","Beijing","China",
            1637064041000,2,"涛思数据taosdata","保利广场baoli","Beijing","China",
            1637064042000,3,"TDengine涛思数据","time广场","NewYork","US",
            1637064043000,4,"4涛思数据","4广场南部","London","UK",
            1637064044000,5,"涛思数据5","!广场路中部123","Tokyo","JP",
            1637064045000,6,"taos涛思数据6","青年广场123号！","Washin","DC",
            1637064046000,7,"7涛思数据taos","asdf#壮年广场%#endregion","NewYork","US",
            1637064047000,8,"8&涛思数据taos","incluse阿斯顿发","NewYork","US",
            1637064048000,9,"&涛思数据taos9","123黑化肥werq会挥……&¥%发！afsdfa","NewYork","US",
            };

            String dropTb = "drop table if exists " + tableName;
            String createTb = $"create table if not exists {tableName} (ts timestamp,v4 int,blob nchar(200),location nchar(200),city binary(100),coutry nchar(200));";
            String insertSql = UtilsTools.ConstructInsertSql(tableName, "", colData, null, 9);
            String selectSql = "select * from " + tableName;           

            UtilsTools.ExecuteUpdate(conn, dropTb);
            UtilsTools.ExecuteUpdate(conn, createTb);
            UtilsTools.ExecuteUpdate(conn, insertSql);
            IntPtr res = UtilsTools.ExecuteQuery(conn, selectSql);

            ResultSet resultSet = new ResultSet(res);
            List<Object> queryResult = resultSet.GetResultData();
            int fieldsCount = resultSet.GetFieldsNum(); 

            //display
            for(int i = 0 ; i<queryResult.Count;i++){
                Console.Write(queryResult[i].ToString());
                Console.Write("\t");
                if((i+1)%fieldsCount == 0)
                {
                   Console.WriteLine(""); 
                }                
            }

            if(colData.Count == queryResult.Count)
            {
                Console.WriteLine("insert data count = retrieve data count");
                for(int i = 0 ; i<queryResult.Count;i++)
                {
                    if(!queryResult[i].Equals(colData[i]))
                    {
                        Console.Write("[Unequal Data]");
                        Console.WriteLine("InsertData:{0},QueryData:{1}",queryResult[i],colData[i]);
                    }
                }
            }
        }
    }
}
