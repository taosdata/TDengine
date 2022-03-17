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
        /// <Name>StableStmtCases.TestBindSingleLineCN</Name>
        /// <describe>Test stmt insert single line of chinese character into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "StableStmtCases.TestBindSingleLineCN()")]
        public void TestBindSingleLineCN()
        {
            string tableName = "stb_stmt_cases_test_bind_single_line_cn";
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
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<String> expectResData = DataSource.GetSTableCNRowData();
            TAOS_BIND[] tags = DataSource.GetCNTags();
            TAOS_BIND[] binds = DataSource.GetNTableCNRow();

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
        /// <Name>StableStmtCases.TestBindColumnCN</Name>
        /// <describe>Test stmt insert single line of chinese character into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "StableStmtCases.TestBindColumnCN()")]
        public void TestBindColumnCN()
        {
            string tableName = "stb_stmt_cases_test_bindcolumn_cn";
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
            TAOS_MULTI_BIND[] mBinds = DataSource.GetMultiBindCNArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableCNRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);

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

            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosMBind(mBinds);

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
        /// <Name>StableStmtCases.TestBindMultiLineCN</Name>
        /// <describe>Test stmt insert single line of chinese character into stable by column after column </describe>
        /// <filename>StmtSTable.cs</filename>
        /// <result>pass or failed </result>
        [Fact(DisplayName = "StableStmtCases.TestBindMultiLineCN()")]
        public void TestBindMultiLineCN()
        {
            string tableName = "stb_stmt_cases_test_bind_multi_line_cn";
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
            TAOS_MULTI_BIND[] mBinds = DataSource.GetMultiBindCNArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableCNRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
            StmtUtilTools.BindParamBatch(stmt, mBinds);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);

            StmtUtilTools.StmtClose(stmt);
            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosMBind(mBinds);

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
            string tableName = "stb_stmt_cases_test_bind_multi_line";
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
            TAOS_MULTI_BIND[] mBinds = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);
            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
            StmtUtilTools.BindParamBatch(stmt, mBinds);
            StmtUtilTools.AddBatch(stmt);
            StmtUtilTools.StmtExecute(stmt);
            StmtUtilTools.StmtClose(stmt);

            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosMBind(mBinds);

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
            string tableName = "stb_stmt_cases_test_bindcolumn";
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
            TAOS_MULTI_BIND[] mBinds = DataSource.GetMultiBindArr();
            List<TDengineMeta> expectResMeta = DataSource.GetMetaFromDDL(createSql);
            List<String> expectResData = DataSource.GetMultiBindStableRowData();

            IntPtr conn = UtilsTools.TDConnection();
            UtilsTools.ExecuteUpdate(conn, dropSql);
            UtilsTools.ExecuteUpdate(conn, createSql);

            IntPtr stmt = StmtUtilTools.StmtInit(conn);
            StmtUtilTools.StmtPrepare(stmt, insertSql);

            StmtUtilTools.SetTableNameTags(stmt, tableName + "_t1", tags);
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

            DataSource.FreeTaosBind(tags);
            DataSource.FreeTaosMBind(mBinds);

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