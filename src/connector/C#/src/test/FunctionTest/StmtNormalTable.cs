using System;
using Test.UtilsTools;
using TDengineDriver;
using Test.UtilsTools.DataSource;
using Xunit;
using System.Collections.Generic;
using Test.UtilsTools.ResultSet;
using Test.Fixture;
using Test.Case.Attributes;

namespace Cases
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]
    public class NormalTableStmtCases
    {
        DatabaseFixture database;

        public NormalTableStmtCases(DatabaseFixture fixture)
        {
            this.database = fixture;
        }
        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindSingleLineCN</Name>
        /// <describe>Test stmt insert single line of chinese character into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "NormalTableStmtCases.TestBindSingleLineCN()"),TestExeOrder(2),Trait("Category", "bindParamCN")]
        public void TestBindSingleLineCN()
        {
            string tableName = "ntb_stmt_cases_test_bind_single_line_cn";
            String createTb = $"create table if not exists {tableName} (" +
                                "ts timestamp," +
                                "tt tinyint," +
                                "si smallint," +
                                "ii int," +
                                "bi bigint," +
                                "tu tinyint unsigned," +
                                "su smallint unsigned," +
                                "iu int unsigned," +
                                "bu bigint unsigned," +
                                "ff float," +
                                "dd double," +
                                "bb binary(200)," +
                                "nc nchar(200)," +
                                "bo bool," +
                                "nullval int" +
                                ");";
            string insertSql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            string dropSql = $"drop table if exists {tableName}";
            string querySql = "select * from " + tableName;
            TAOS_BIND[] _valuesRow = DataSource.GetNTableCNRow();
            List<string> expectResData = DataSource.GetNTableCNRowData();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            IntPtr conn = database.conn;
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);
            StmtUtilTools.BindParam(stmt, _valuesRow);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosBind(_valuesRow);

            IntPtr res = UtilsTools.ExecuteQuery(conn, querySql);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.GetResultMeta();
            List<string> actualResData = actualResult.GetResultData();

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert metadata
            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
        }

        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindColumnCN</Name>
        /// <describe>Test stmt insert single line of chinese character into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindColumnCN()"),TestExeOrder(4),Trait("Category", "bindSingleColumnCN")]
        public void TestBindColumnCN()
        {
            string tableName = "ntb_stmt_cases_test_bind_column_cn";
            String createTb = $"create table if not exists {tableName} " +
                                " (" +
                                "ts timestamp," +
                                "b bool," +
                                "v1 tinyint," +
                                "v2 smallint," +
                                "v4 int," +
                                "v8 bigint," +
                                "f4 float," +
                                "f8 double," +
                                "u1 tinyint unsigned," +
                                "u2 smallint unsigned," +
                                "u4 int unsigned," +
                                "u8 bigint unsigned," +
                                "bin binary(200)," +
                                "blob nchar(200)" +
                                ");";
            String insertSql = "insert into ?  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName} ";
            List<string> expectResData = DataSource.GetMultiBindCNRowData();
            TAOS_MULTI_BIND[] mBind = DataSource.GetMultiBindCNArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            IntPtr conn = database.conn;
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);

            StmtUtilTools.BindSingleParamBatch(stmt, mBind[0], 0);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[1], 1);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[2], 2);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[3], 3);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[4], 4);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[5], 5);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[6], 6);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[7], 7);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[8], 8);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[9], 9);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[10], 10);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[11], 11);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[12], 12);
            StmtUtilTools.BindSingleParamBatch(stmt, mBind[13], 13);

            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosMBind(mBind);

            string querySql = "select * from " + tableName;
            IntPtr res = UtilsTools.ExecuteQuery(conn, querySql);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.GetResultMeta();
            List<string> actualResData = actualResult.GetResultData();
            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert metadata
            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);

            }
        }

        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindMultiLineCN</Name>
        /// <describe>Test stmt insert single line of chinese character into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindMultiLineCN()"),TestExeOrder(6),Trait("Category", "bindParamBatchCN")]
        public void TestBindMultiLineCN()
        {
            string tableName = "ntb_stmt_cases_test_bind_multi_lines_cn";
            TAOS_MULTI_BIND[] mBind = DataSource.GetMultiBindCNArr();
            String createTb = $"create table if not exists {tableName} " +
                                " (" +
                                "ts timestamp," +
                                "b bool," +
                                "v1 tinyint," +
                                "v2 smallint," +
                                "v4 int," +
                                "v8 bigint," +
                                "f4 float," +
                                "f8 double," +
                                "u1 tinyint unsigned," +
                                "u2 smallint unsigned," +
                                "u4 int unsigned," +
                                "u8 bigint unsigned," +
                                "bin binary(200)," +
                                "blob nchar(200)" +
                                ");";
            String insertSql = "insert into ?  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName} ";
            List<string> expectResData = DataSource.GetMultiBindCNRowData();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            IntPtr conn = database.conn; ;
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);
            StmtUtilTools.BindParamBatch(stmt, mBind);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosMBind(mBind);

            string querySql = "select * from " + tableName;
            IntPtr res = UtilsTools.ExecuteQuery(conn, querySql);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.GetResultMeta();
            List<string> actualResData = actualResult.GetResultData();
            Assert.Equal(expectResMeta.Count, actualResMeta.Count);
            Assert.Equal(expectResData.Count, actualResData.Count);

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert metadata
            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
        }

        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindSingleLine</Name>
        /// <describe>Test stmt insert single line data into normal table</describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "NormalTableStmtCases.TestBindSingleLine"),TestExeOrder(3),Trait("Category", "BindSingleColumn")]
        public void TestBindSingleLine()
        {
            string tableName = "ntb_stmt_cases_test_bind_single_line";
            String createTb = $"create table if not exists {tableName} (" +
                                "ts timestamp," +
                                "tt tinyint," +
                                "si smallint," +
                                "ii int," +
                                "bi bigint," +
                                "tu tinyint unsigned," +
                                "su smallint unsigned," +
                                "iu int unsigned," +
                                "bu bigint unsigned," +
                                "ff float," +
                                "dd double," +
                                "bb binary(200)," +
                                "nc nchar(200)," +
                                "bo bool," +
                                "nullval int" +
                                ");";
            string insertSql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            string dropSql = $"drop table if exists {tableName}";
            string querySql = "select * from " + tableName;
            TAOS_BIND[] valuesRow = DataSource.GetNTableRow();
            List<string> expectResData = DataSource.GetNTableRowData();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            IntPtr conn = database.conn;
            UtilsTools.ExecuteQuery(conn, dropSql);
            UtilsTools.ExecuteQuery(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);
            StmtUtilTools.BindParam(stmt, valuesRow);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosBind(valuesRow);

            IntPtr res = UtilsTools.ExecuteQuery(conn, querySql);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.GetResultMeta();
            List<string> actualResData = actualResult.GetResultData();
            Assert.Equal(expectResMeta.Count, actualResMeta.Count);
            Assert.Equal(expectResData.Count, actualResData.Count);

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert metadata
            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }

        }

        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindMultiLine</Name>
        /// <describe>Test stmt insert multiple rows of data into normal table</describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindMultiLine()"),TestExeOrder(5),Trait("Category", "bindParamBatch")]
        public void TestBindMultiLine()
        {
            string tableName = "ntb_stmt_case_test_bind_multi_lines";
            String createTb = $"create table if not exists {tableName} " +
                                " (" +
                                "ts timestamp," +
                                "b bool," +
                                "v1 tinyint," +
                                "v2 smallint," +
                                "v4 int," +
                                "v8 bigint," +
                                "f4 float," +
                                "f8 double," +
                                "u1 tinyint unsigned," +
                                "u2 smallint unsigned," +
                                "u4 int unsigned," +
                                "u8 bigint unsigned," +
                                "bin binary(200)," +
                                "blob nchar(200)" +
                                ");";
            String insertSql = "insert into ?  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName} ";
            List<string> expectResData = DataSource.GetMultiBindResData();
            TAOS_MULTI_BIND[] mBind = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);

            IntPtr conn = database.conn;
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);


            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);
            StmtUtilTools.BindParamBatch(stmt, mBind);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosMBind(mBind);

            string querySql = "select * from " + tableName;
            IntPtr res = UtilsTools.ExecuteQuery(conn, querySql);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.GetResultMeta();
            List<string> actualResData = actualResult.GetResultData();
            Assert.Equal(expectResMeta.Count, actualResMeta.Count);
            Assert.Equal(expectResData.Count, actualResData.Count);

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert metadata
            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }
        }

        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindColumn</Name>
        /// <describe>Test stmt insert multiple rows of data into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindColumn()"),TestExeOrder(1),Trait("Category", "bindParam")]
        public void TestBindColumn()
        {
            string tableName = "ntb_stmt_cases_test_bind_column";
            DataSource data = new DataSource();
            String createTb = $"create table if not exists {tableName} " +
                                " (" +
                                "ts timestamp," +
                                "b bool," +
                                "v1 tinyint," +
                                "v2 smallint," +
                                "v4 int," +
                                "v8 bigint," +
                                "f4 float," +
                                "f8 double," +
                                "u1 tinyint unsigned," +
                                "u2 smallint unsigned," +
                                "u4 int unsigned," +
                                "u8 bigint unsigned," +
                                "bin binary(200)," +
                                "blob nchar(200)" +
                                ");";
            String insertSql = "insert into ?  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName} ";
            List<string> expectResData = DataSource.GetMultiBindResData();
            TAOS_MULTI_BIND[] mBinds = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createTb);


            IntPtr conn = database.conn;
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);

            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[0], 0);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[1], 1);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[2], 2);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[3], 3);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[4], 4);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[5], 5);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[6], 6);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[7], 7);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[8], 8);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[9], 9);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[10], 10);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[11], 11);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[12], 12);
            StmtUtilTools.BindSingleParamBatch(stmt, mBinds[13], 13);

            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosMBind(mBinds);

            string querySql = "select * from " + tableName;
            IntPtr res = UtilsTools.ExecuteQuery(conn, querySql);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.GetResultMeta();
            List<string> actualResData = actualResult.GetResultData();
            Assert.Equal(expectResMeta.Count, actualResMeta.Count);
            Assert.Equal(expectResData.Count, actualResData.Count);

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                Assert.Equal(expectResData[i], actualResData[i]);
            }
            // Assert metadata
            for (int i = 0; i < actualResMeta.Count; i++)
            {
                Assert.Equal(expectResMeta[i].name, actualResMeta[i].name);
                Assert.Equal(expectResMeta[i].type, actualResMeta[i].type);
                Assert.Equal(expectResMeta[i].size, actualResMeta[i].size);
            }

        }

    }
}