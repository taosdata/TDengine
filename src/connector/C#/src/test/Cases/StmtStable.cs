using System;
using Test.UtilsTools;
using TDengineDriver;
using Test.UtilsTools.DataSource;

namespace Cases
{

    public class StableMutipleLine
    {
        TAOS_BIND[] tags = DataSource.getTags();
        TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindArr();
        public void Test(IntPtr conn, string tableName)
        {
            String createTb = "create stable " + tableName + " (ts timestamp ,b bool,v1 tinyint,v2 smallint,v4 int,v8 bigint,f4 float,f8 double,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,bin binary(200),blob nchar(200))tags(bo bool,tt tinyint,si smallint,ii int,bi bigint,tu tinyint unsigned,su smallint unsigned,iu int unsigned,bu bigint unsigned,ff float ,dd double ,bb binary(200),nc nchar(200));";
            String insertSql = "insert into ? using " + tableName + " tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            UtilsTools.ExecuteQuery(conn, createTb);
            String[] loadList = { tableName };
            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.loadTableInfo(conn, loadList);

            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
            StmtUtilTools.BindParamBatch(stmt, mbind);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);

            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosMBind(mbind);
        }
    }
    public class StableColumnByColumn
    {
        DataSource data = new DataSource();

        TAOS_BIND[] tags = DataSource.getTags();
        TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindArr();
        public void Test(IntPtr conn, string tableName)
        {
            String createTb = "create stable " + tableName + " (ts timestamp ,b bool,v1 tinyint,v2 smallint,v4 int,v8 bigint,f4 float,f8 double,u1 tinyint unsigned,u2 smallint unsigned,u4 int unsigned,u8 bigint unsigned,bin binary(200),blob nchar(200))tags(bo bool,tt tinyint,si smallint,ii int,bi bigint,tu tinyint unsigned,su smallint unsigned,iu int unsigned,bu bigint unsigned,ff float ,dd double ,bb binary(200),nc nchar(200));";
            String insertSql = "insert into ? using " + tableName + " tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";


            UtilsTools.ExecuteQuery(conn, createTb);
            IntPtr stmt = StmtUtilTools.StmtInit(conn);

            StmtUtilTools.StmtPrepare(stmt, insertSql);

            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[0], 0);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[1], 1);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[2], 2);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[3], 3);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[4], 4);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[5], 5);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[6], 6);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[7], 7);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[8], 8);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[9], 9);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[10], 10);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[11], 11);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[12], 12);
            StmtUtilTools.BindSingleParamBatch(stmt, mbind[13], 13);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosMBind(mbind);

        }
    }

}