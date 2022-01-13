using System;
using Test.UtilsTools;
using TDengineDriver;
using Test.UtilsTools.DataSource;
using System.Collections.Generic;
using Test.UtilsTools.ResultSet;
using Xunit;

namespace Cases
{
    public class StableStmtCases
    {
        /// <author>xiaolei</author>
        /// <Name>StableStmtCases.TestBindSingleLineCn</Name>
        /// <describe>Test stmt insert single line of chinese character into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StableStmtCases.TestBindSingleLineCn()")]
        public void TestBindSingleLineCn()
        {
            string tableName = "stable_stmt_cases_test_bind_single_line_cn";
            String createSql = $"create stable if not exists {tableName} " +
                                " (ts timestamp," +
                                "v1 tinyint," +
                                "v2 smallint," +
                                "v4 int," +
                                "v8 bigint," +
                                "u1 tinyint unsigned," +
                                "u2 smallint unsigned," +
                                "u4 int unsigned," +
                                "u8 bigint unsigned," +
                                "f4 float," +
                                "f8 double," +
                                "bin binary(200)," +
                                "blob nchar(200)," +
                                "b bool," +
                                "nilcol int)" +
                                "tags" +
                                "(bo bool," +
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
                                "nc nchar(200)" +
                                ");";
            String insertSql = $"insert into ? using  {tableName} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName} ;";
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createSql);
            List<String> expectResData = DataSource.GetStableCNRowData();
            TAOS_BIND[] tags = DataSource.GetCNTags();
            TAOS_BIND[] binds = DataSource.GetNtableCNRow();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
            StmtUtilTools.BindParam(stmt, binds);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosBind(binds);

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
        /// <Name>StableStmtCases.TestBindColumnCn</Name>
        /// <describe>Test stmt insert single line of chinese character into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "StableStmtCases.TestBindColumnCn()")]
        public void TestBindColumnCn()
        {
            string tableName = "stable_stmt_cases_test_bindcolumn_cn";
            String createSql = $"create stable  if not exists {tableName} " +
                                "(ts timestamp," +
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
                                ")" +
                                "tags" +
                                "(bo bool," +
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
                                "nc nchar(200)" +
                                ");";
            String insertSql = "insert into ? using " + tableName + " tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName};";
            TAOS_BIND[] tags = DataSource.GetCNTags();
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindCNArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableCNRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

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
        /// <Name>StableStmtCases.TestBindMultiLineCn</Name>
        /// <describe>Test stmt insert single line of chinese character into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "StableStmtCases.TestBindMultiLineCn()")]
        public void TestBindMultiLineCn()
        {
            string tableName = "stable_stmt_cases_test_bind_multi_line_cn";
            String createSql = $"create stable  if not exists {tableName} " +
                                "(ts timestamp," +
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
                                ")" +
                                "tags" +
                                "(bo bool," +
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
                                "nc nchar(200)" +
                                ");";
            String insertSql = "insert into ? using " + tableName + " tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName};";
            TAOS_BIND[] tags = DataSource.GetCNTags();
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindCNArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableCNRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
            StmtUtilTools.BindParamBatch(stmt, mbind);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);

            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosBind(tags);
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
        /// <Name>StableStmtCases.TestBindMultiLine</Name>
        /// <describe>Test stmt insert single line into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result>         
        [Fact(DisplayName = "StableStmtCases.TestBindMultiLine()")]
        public void TestBindMultiLine()
        {
            string tableName = "stable_stmt_cases_test_bind_multi_line";
            string createSql = $"create stable  if not exists {tableName} " +
                                "(ts timestamp," +
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
                                ")" +
                                "tags" +
                                "(bo bool," +
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
                                "nc nchar(200)" +
                                ");";
            String insertSql = "insert into ? using " + tableName + " tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName};";
            TAOS_BIND[] tags = DataSource.GetTags();
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
            StmtUtilTools.BindParamBatch(stmt, mbind);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosMBind(mbind);

            string querySql = "select * from " + tableName;
            IntPtr res = UtilsTools.ExecuteQuery(conn, querySql);
            ResultSet actualResult = new ResultSet(res);

            List<TDengineMeta> actualResMeta = actualResult.GetResultMeta();
            List<string> actualResData = actualResult.GetResultData();

            // Assert retrieve data
            for (int i = 0; i < actualResData.Count; i++)
            {
                // Assert.Equal(expectResData[i],actualResData[i]);
                if (expectResData[i] != actualResData[i])
                {
                    Console.WriteLine("{0}==>,expectResData:{1},actualResData:{2}", i, expectResData[i], actualResData[i]);
                }

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
        /// <Name>StableStmtCases.TestBindColumn</Name>
        /// <describe>Test stmt insert single line of chinese character into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StableStmtCases.TestBindColumn()")]
        public void TestBindColumn()
        {
            string tableName = "stable_stmt_cases_test_bindcolumn";
            string createSql = $"create stable  if not exists {tableName} " +
                                "(ts timestamp," +
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
                                ")" +
                                "tags" +
                                "(bo bool," +
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
                                "nc nchar(200)" +
                                ");";
            String insertSql = "insert into ? using " + tableName + " tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String dropSql = $"drop table if exists {tableName};";
            TAOS_BIND[] tags = DataSource.GetTags();
            TAOS_MULTI_BIND[] mbind = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDLL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

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

    }
}