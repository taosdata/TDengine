using System;
using System.Collections.Generic;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;
using Test.Case.Attributes;
using Test.Fixture;
using Test.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Function.Test.Taosc
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]
    public class Query
    {
        readonly DatabaseFixture database;
        private readonly ITestOutputHelper _output;

        public Query(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }

        /// <author>xiaolei</author>
        /// <Name>Query.NormalTable</Name>
        /// <describe>Insert data into normal table and query data.</describe>
        /// <filename>Query.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "Query.NormalTable"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            IntPtr conn = database.Conn;
            string tableName = "query_tn";
            string createTable = Tools.CreateTable(tableName, false, false);
            List<Object> columns = Tools.ColumnsList(5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTable);
            string insertSql = Tools.ConstructInsertSql(tableName, "", columns, null, 5);
            string selectSql = $"select * from {tableName}";

            // prepare 
            Tools.ExecuteUpdate(conn, createTable, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            // assert 
            IntPtr res = Tools.ExecuteQuery(conn, selectSql, _output);
            List<TDengineMeta> actualResMeta = NativeMethods.FetchFields(res);
            List<object> actualResData = Tools.GetData(res);

            _output.WriteLine("Assert meta");
            expectResMeta.ForEach(meta =>
            {
                Assert.Equal(meta.name, actualResMeta[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.type, actualResMeta[expectResMeta.IndexOf(meta)].type);
                Assert.Equal(meta.size, actualResMeta[expectResMeta.IndexOf(meta)].size);
            });

            _output.WriteLine("Assert data");
            for (int i = 0; i < columns.Count; i++)
            {
                //_output.WriteLine("{0},{1},{2}",i, columns[i], actualResData[i]);
                var val = actualResData[i];
                var expectVal = columns[i];
                if (val is float floatVal)
                {
                    Assert.IsType<float>(expectVal);
                    Assert.Equal((float)expectVal, floatVal, 7);
                }
                else if (val is double doubleVal)
                {
                    Assert.IsType<double>(expectVal);
                    Assert.Equal((double)expectVal, doubleVal, 15);
                }
                else
                {
                    Assert.Equal(expectVal, val);
                }
            }

            Tools.FreeResult(res);
        }

        /// <author>xiaolei</author>
        /// <Name>Query.STable</Name>
        /// <describe>Insert data into STable table and query data.</describe>
        /// <filename>Query.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "Query.STable"), TestExeOrder(2), Trait("Category", "STable")]
        public void STable()
        {
            IntPtr conn = database.Conn;
            string tableName = "query_st";
            string createSql = Tools.CreateTable(tableName, true, false);
            List<object> columns = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1, false);
            List<Object> expectResData = Tools.ConstructResData(columns, tags, 5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            string insertSql = Tools.ConstructInsertSql($"{tableName}_s1", tableName, columns, tags, 5);
            string selectSql = $"select * from {tableName}";

            // prepare 
            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            // Assert 
            IntPtr res = Tools.ExecuteQuery(conn, selectSql, _output);
            List<TDengineMeta> actualResMeta = NativeMethods.FetchFields(res);
            List<object> actualResData = Tools.GetData(res);


            _output.WriteLine("Assert meta");
            expectResMeta.ForEach(meta =>
            {
                Assert.Equal(meta.name, actualResMeta[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.type, actualResMeta[expectResMeta.IndexOf(meta)].type);
                Assert.Equal(meta.size, actualResMeta[expectResMeta.IndexOf(meta)].size);
            });

            _output.WriteLine("Assert data");
            for (int i = 0; i < expectResData.Count; i++)
            {
                //_output.WriteLine("{0},{1},{2}", i, expectResData[i], actualResData[i]);
                var val = actualResData[i];
                var expectVal = expectResData[i];
                if (val is float floatVal)
                {
                    Assert.IsType<float>(expectVal);
                    Assert.Equal((float)expectVal, floatVal, 7);
                }
                else if (val is double doubleVal)
                {
                    Assert.IsType<double>(expectVal);
                    Assert.Equal((double)expectVal, doubleVal, 15);
                }
                else
                {
                    Assert.Equal(expectVal, val);
                }
            }

            Tools.FreeResult(res);
        }

        /// <author>xiaolei</author>
        /// <Name>Query.JSONTag</Name>
        /// <describe>Insert data into table which has JSON tag and query data.</describe>
        /// <filename>Query.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "Query.JSONTag"), TestExeOrder(3), Trait("Category", "JSONTag")]
        public void JSONTag()
        {
            IntPtr conn = database.Conn;
            string tableName = "query_sj";
            string createSql = Tools.CreateTable(tableName, true, true);
            List<object> columns = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1, true);
            List<Object> expectResData = Tools.ConstructResData(columns, tags, 5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            string insertSql = Tools.ConstructInsertSql($"{tableName}_j1", tableName, columns, tags, 5);
            string selectSql = $"select * from {tableName}";

            // prepare 
            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            // Assert 
            IntPtr res = Tools.ExecuteQuery(conn, selectSql, _output);
            List<TDengineMeta> actualResMeta = NativeMethods.FetchFields(res);
            List<object> actualResData = Tools.GetData(res);


            _output.WriteLine("Assert meta");
            expectResMeta.ForEach(meta =>
            {
                Assert.Equal(meta.name, actualResMeta[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.type, actualResMeta[expectResMeta.IndexOf(meta)].type);
                Assert.Equal(meta.size, actualResMeta[expectResMeta.IndexOf(meta)].size);
            });

            _output.WriteLine("Assert data");
            for (int i = 0; i < expectResData.Count; i++)
            {
                //_output.WriteLine("{0},{1},{2}", i, expectResData[i], actualResData[i]);
                var val = actualResData[i];
                var expectVal = expectResData[i];
                if (val is float floatVal)
                {
                    Assert.IsType<float>(expectVal);
                    Assert.Equal((float)expectVal, floatVal, 7);
                }
                else if (val is double doubleVal)
                {
                    Assert.IsType<double>(expectVal);
                    Assert.Equal((double)expectVal, doubleVal, 15);
                }
                else
                {
                    Assert.Equal(expectVal, val);
                }
            }

            Tools.FreeResult(res);
        }

        [Fact(DisplayName = "Query.ReqId"), TestExeOrder(4), Trait("Category", "ReqId")]
        public void ReqId()
        {
            IntPtr conn = database.Conn;
            string tableName = "query_req_id";
            string createTable = Tools.CreateTable(tableName, false, false);
            List<Object> columns = Tools.ColumnsList(5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createTable);
            string insertSql = Tools.ConstructInsertSql(tableName, "", columns, null, 5);
            string selectSql = $"select * from {tableName}";

            // prepare 
            Tools.ExecuteUpdate(conn, createTable, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            // assert 
            IntPtr res = Tools.ExecuteQueryWithReqId(conn, selectSql, _output, TDengine.Driver.ReqId.GetReqId());
            List<TDengineMeta> actualResMeta = NativeMethods.FetchFields(res);
            List<object> actualResData = Tools.GetData(res);

            _output.WriteLine("Assert meta");
            expectResMeta.ForEach(meta =>
            {
                Assert.Equal(meta.name, actualResMeta[expectResMeta.IndexOf(meta)].name);
                Assert.Equal(meta.type, actualResMeta[expectResMeta.IndexOf(meta)].type);
                Assert.Equal(meta.size, actualResMeta[expectResMeta.IndexOf(meta)].size);
            });

            _output.WriteLine("Assert data");
            for (int i = 0; i < columns.Count; i++)
            {
                //_output.WriteLine("{0},{1},{2}",i, columns[i], actualResData[i]);
                var val = actualResData[i];
                var expectVal = columns[i];
                if (val is float floatVal)
                {
                    Assert.IsType<float>(expectVal);
                    Assert.Equal((float)expectVal, floatVal, 7);
                }
                else if (val is double doubleVal)
                {
                    Assert.IsType<double>(expectVal);
                    Assert.Equal((double)expectVal, doubleVal, 15);
                }
                else
                {
                    Assert.Equal(expectVal, val);
                }
            }

            Tools.FreeResult(res);
        }
    }
}