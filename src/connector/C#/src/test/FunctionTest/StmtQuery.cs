using System;
using Test.UtilsTools;
using TDengineDriver;
using Test.UtilsTools.DataSource;

namespace Cases
{
    public class StmtSTableQuery
    {
        public void Test(IntPtr conn, string tableName)
        {
            string selectSql = "SELECT * FROM " + tableName + " WHERE v1 > ? AND v4 < ?";
            TAOS_BIND[] queryCondition = DataSource.GetQueryCondition();

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, selectSql);

            StmtUtilTools.BindParam(stmt, queryCondition);
            StmtUtilTools.StmtExecute(stmt);
            IntPtr res = StmtUtilTools.StmtUseResult(stmt);
            UtilsTools.DisplayRes(res);

            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosBind(queryCondition);

        }
    }

    public class StmtNTableQuery
    {
        public void Test(IntPtr conn, string tableName)
        {
            string selectSql = "SELECT * FROM " + tableName + " WHERE v1 > ? AND v4 < ?";
            TAOS_BIND[] queryCondition = DataSource.GetQueryCondition();

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, selectSql);

            StmtUtilTools.BindParam(stmt, queryCondition);
            StmtUtilTools.StmtExecute(stmt);
            IntPtr res = StmtUtilTools.StmtUseResult(stmt);
            UtilsTools.DisplayRes(res);

            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosBind(queryCondition);

        }
    }
}