using System;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace Test.UtilsTools
{
    public class StmtUtilTools
    {
        public static IntPtr StmtInit(IntPtr conn)
        {
            IntPtr stmt = TDengine.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                Console.WriteLine("Init stmt failed");
                UtilsTools.CloseConnection(conn);
                UtilsTools.ExitProgram();
            }
            else
            {
                Console.WriteLine("Init stmt success");
            }
            return stmt;
        }

        public static void StmtPrepare(IntPtr stmt, string sql)
        {
            int res = TDengine.StmtPrepare(stmt, sql);
            if (res == 0)
            {
                Console.WriteLine("stmt prepare success");
            }
            else
            {
                Console.WriteLine("stmt prepare failed " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void SetTableName(IntPtr stmt, String tableName)
        {
            int res = TDengine.StmtSetTbname(stmt, tableName);
            if (res == 0)
            {
                Console.WriteLine("set_tbname success");
            }
            else
            {
                Console.Write("set_tbname failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void SetTableNameTags(IntPtr stmt, String tableName, TAOS_BIND[] tags)
        {
            int res = TDengine.StmtSetTbnameTags(stmt, tableName, tags);
            if (res == 0)
            {
                Console.WriteLine("set tbname && tags success");

            }
            else
            {
                Console.Write("set tbname && tags failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void SetSubTableName(IntPtr stmt, string name)
        {
            int res = TDengine.StmtSetSubTbname(stmt, name);
            if (res == 0)
            {
                Console.WriteLine("set subtable name success");
            }
            else
            {
                Console.Write("set subtable name failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }

        }

        public static void BindParam(IntPtr stmt, TAOS_BIND[] binds)
        {
            int res = TDengine.StmtBindParam(stmt, binds);
            if (res == 0)
            {
                Console.WriteLine("bind  para success");
            }
            else
            {
                Console.Write("bind  para failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void BindSingleParamBatch(IntPtr stmt, TAOS_MULTI_BIND bind, int index)
        {
            int res = TDengine.StmtBindSingleParamBatch(stmt, ref bind, index);
            if (res == 0)
            {
                Console.WriteLine("single bind  batch success");
            }
            else
            {
                Console.Write("single bind  batch failed: " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void BindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind)
        {
            int res = TDengine.StmtBindParamBatch(stmt, bind);
            if (res == 0)
            {
                Console.WriteLine("bind  parameter batch success");
            }
            else
            {
                Console.WriteLine("bind  parameter batch failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void AddBatch(IntPtr stmt)
        {
            int res = TDengine.StmtAddBatch(stmt);
            if (res == 0)
            {
                Console.WriteLine("stmt add batch success");
            }
            else
            {
                Console.Write("stmt add batch failed,reason: " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }
        public static void StmtExecute(IntPtr stmt)
        {
            int res = TDengine.StmtExecute(stmt);
            if (res == 0)
            {
                Console.WriteLine("Execute stmt success");
            }
            else
            {
                Console.Write("Execute stmt failed,reason: " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }
        public static void StmtClose(IntPtr stmt)
        {
            int res = TDengine.StmtClose(stmt);
            if (res == 0)
            {
                Console.WriteLine("close stmt success");
            }
            else
            {
                Console.WriteLine("close stmt failed, " + TDengine.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static IntPtr StmtUseResult(IntPtr stmt)
        {
            IntPtr res = TDengine.StmtUseResult(stmt);
            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));
                }
                Console.WriteLine("");
                StmtClose(stmt);
            }
            else
            {
                Console.WriteLine("StmtUseResult success");

            }
            return res;
        }

        public static void loadTableInfo(IntPtr conn, string[] arr)
        {
            if (TDengine.LoadTableInfo(conn, arr) == 0)
            {
                Console.WriteLine("load table info success");
            }
            else
            {
                Console.WriteLine("load table info failed");
            }
        }

    }
}