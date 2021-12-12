using System;
using Test.UtilsTools;
using TDengineDriver;
using System.Collections.Generic;
using System.Runtime.InteropServices;
namespace Cases
{
    public class FetchFields
    {
        public void Test(IntPtr conn, string tableName)
        {
            IntPtr res = IntPtr.Zero;
            String createTb = "create stable " + tableName + " (ts timestamp ,b bool,v1 tinyint,v2 smallint,v4 int,v8 bigint,f4 float,f8 double,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,bin binary(200),blob nchar(200))tags(id int);";
            String insertSql = "insert into " + tableName + "_t1 using " + tableName + " tags(1) values(1637064040000,true,1,2,3,4,5,6,7,8,9,10,'XI','XII')";
            String selectSql = "select * from " + tableName;
            String dropSql = "drop table " + tableName;
            UtilsTools.ExecuteQuery(conn, createTb);
            UtilsTools.ExecuteQuery(conn, insertSql);
            res = UtilsTools.ExecuteQuery(conn, selectSql);
            UtilsTools.ExecuteQuery(conn, dropSql);

            List<TDengineMeta> metas = new List<TDengineMeta>();
            metas = TDengine.FetchFields(res);
            if (metas.Capacity == 0)
            {
                Console.WriteLine("empty result");
            }
            else
            {
                foreach(TDengineMeta meta in metas){
                    Console.WriteLine("col_name:{0},col_type_code:{1},col_type:{2}({3})",meta.name,meta.type,meta.TypeName(),meta.size);
                }
            }

        }
    }
}