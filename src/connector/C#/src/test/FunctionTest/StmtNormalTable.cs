using System;
using Test.UtilsTools;
using TDengineDriver;
using Test.UtilsTools.DataSource;
using Xunit;
using System.Collections.Generic;
using Test.UtilsTools.ResultSet;
namespace Cases
{
    public class NormalTableStmtCases
    {
        /// <author>xiaolei</author>
        /// <Name>NormalTableStmtCases.TestBindSingleLineCn</Name>
        /// <describe>Test stmt insert single line of chinese character into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result>  
        [Fact(DisplayName = "NormalTableStmtCases.TestBindSingleLineCn()")]
        public void TestBindSingleLineCn()
        {
            string tableName = "normal_tablestmt_cases_test_bind_single_line_cn";
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
            TAOS_BIND[] _valuesRow = DataSource.GetNtableCNRow();
            List<string> expectResData = DataSource.GetNtableCNRowData();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createTb);

            IntPtr conn = UtilsTools.TDConnection();
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
        /// <Name>NormalTableStmtCases.TestBindColumnCn</Name>
        /// <describe>Test stmt insert single line of chinese character into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindColumnCn()")]
        public void TestBindColumnCn()
        {
            string tableName = "normal_tablestmt_cases_test_bind_column_cn";
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
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindCNArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createTb);

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);

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

            DataSource.FreeTaosMBind(mbind);

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
        /// <Name>NormalTableStmtCases.TestBindMultiLineCn</Name>
        /// <describe>Test stmt insert single line of chinese character into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindMultiLineCn()")]
        public void TestBindMultiLineCn()
        {
            string tableName = "normal_tablestmt_cases_test_bind_multi_lines_cn";
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindCNArr();
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
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createTb);

            IntPtr conn = UtilsTools.TDConnection(); ;
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);
            StmtUtilTools.BindParamBatch(stmt, mbind);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosMBind(mbind);

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
        /// <describe>Test stmt insert sinle line data into normal table</describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "NormalTableStmtCases.TestBindSingleLine")]
        public void TestBindSingleLine()
        {
            string tableName = "normal_tablestmt_cases_test_bind_single_line";
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
            TAOS_BIND[] valuesRow = DataSource.GetNtableRow();
            List<string> expectResData = DataSource.GetNtableRowData();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createTb);

            IntPtr conn = UtilsTools.TDConnection();
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
        /// <Name>NtableMutipleLine.TestBindMultiLine</Name>
        /// <describe>Test stmt insert multiple rows of data into normal table</describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindMultiLine()")]
        public void TestBindMultiLine()
        {
            string tableName = "normal_table_stmt_cases_test_bind_multi_lines";
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
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createTb);

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);


            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);
            StmtUtilTools.BindParamBatch(stmt, mbind);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosMBind(mbind);

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
        /// <Name>NtableColumnByColumn.TestBindColumnCn</Name>
        /// <describe>Test stmt insert multiple rows of data into normal table by column after column </describe>
        /// <filename>StmtNormalTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "NormalTableStmtCases.TestBindColumn()")]
        public void TestBindColumn()
        {
            string tableName = "normal_tablestmt_cases_test_bind_column_cn";
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
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createTb);


            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createTb);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableName(stmt, tableName);

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

            DataSource.FreeTaosMBind(mbind);

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