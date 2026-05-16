using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Driver.Test.Function.Test;
using TDengine.Driver;
using TDengine.Driver.Impl;
using Test.Case.Attributes;
using Test.Fixture;
using Test.Utils;
using Test.Utils.ResultSet;
using Xunit;
using Xunit.Abstractions;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace Function.Test.Taosc
{
    [TestCaseOrderer("XUnit.Case.Orderers.TestExeOrderer", "Cases.ExeOrder")]
    [Collection("Database collection")]

    public class QueryAsync
    {
        readonly DatabaseFixture database;

        private readonly ITestOutputHelper _output;

        public QueryAsync(DatabaseFixture fixture, ITestOutputHelper output)
        {
            this.database = fixture;
            this._output = output;
        }
        /// <author>xiaolei</author>
        /// <Name>QueryAsync.NormalTable</Name>
        /// <describe>Test query_a from normal table</describe>
        /// <filename>QueryAsync.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "QueryAsync.NormalTable()"), TestExeOrder(1), Trait("Category", "NormalTable")]
        public void NormalTable()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;

            var tableName = "query_a_tn";
            string createSql = Tools.CreateTable(tableName, false, false);
            List<object> coldata = Tools.ColumnsList(5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            string insertSql = Tools.ConstructInsertSql(tableName, "", coldata, null, 5);
            string selectSql = $"select * from {tableName}";

            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            QueryAsyncCallback fq = new QueryAsyncCallback(QueryCallback);
            NativeMethods.QueryAsync(conn, selectSql, fq, IntPtr.Zero);
            Thread.Sleep(2000);

            void QueryCallback(IntPtr param, IntPtr taosRes, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                    NativeMethods.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
                }
                else
                {
                    _output.WriteLine($"async query data failed, failed code {code}");
                }

            }

            void FetchRawBlockCallback(IntPtr param, IntPtr taosRes, int numOfRows)
            {
                if (numOfRows > 0)
                {

                    IntPtr pdata = NativeMethods.GetRawBlock(taosRes);
                    List<TDengineMeta> actualMeta = NativeMethods.FetchFields(taosRes);
                    List<object> actualResData = Tools.ReadRawBlock(pdata, actualMeta, numOfRows);

                    //Assert Meta data
                    _output.WriteLine("Assert Meta data");
                    for (int i = 0; i < actualMeta.Count; i++)
                    {
                        Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                        Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                        Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
                    }
                    // Assert retrieve data
                    _output.WriteLine("Assert retrieve data");

                    for (int i = 0; i < actualResData.Count; i++)
                    {
                        //_output.WriteLine("{0},{1},{2}", i, coldata[i], actualResData[i]);
                        Assert.Equal(coldata[i], actualResData[i]);
                    }

                    NativeMethods.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
                }
                else
                {
                    if (numOfRows == 0)
                    {
                        _output.WriteLine("async retrieve complete.");

                    }
                    else
                    {
                        _output.WriteLine($"FetchRowAsync callback error, error code {numOfRows}");
                    }
                    NativeMethods.FreeResult(taosRes);
                }
            }

        }

        /// <author>xiaolei</author>
        /// <Name>QueryAsync.QueryWithCondition</Name>
        /// <describe>Test query_a with STable </describe>
        /// <filename>QueryAsync.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "QueryAsync.STable()"), TestExeOrder(2), Trait("Category", "STable")]
        public void STable()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;

            var tableName = "query_a_ts";
            string createSql = Tools.CreateTable(tableName, true, false);
            List<object> coldata = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1, false);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<object> expectResData = Tools.ConstructResData(coldata, tags, 5);
            string insertSql = Tools.ConstructInsertSql($"{tableName}_s1", tableName, coldata, tags, 5);
            string selectSql = $"select * from {tableName}";

            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            QueryAsyncCallback fq = new QueryAsyncCallback(QueryCallback);
            NativeMethods.QueryAsync(conn, selectSql, fq, IntPtr.Zero);
            Thread.Sleep(2000);

            void QueryCallback(IntPtr param, IntPtr taosRes, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                    NativeMethods.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
                }
                else
                {
                    _output.WriteLine($"async query data failed, failed code {code}");
                }

            }

            void FetchRawBlockCallback(IntPtr param, IntPtr taosRes, int numOfRows)
            {
                if (numOfRows > 0)
                {
                    
                    IntPtr pdata = NativeMethods.GetRawBlock(taosRes);
                    List<TDengineMeta> actualMeta = NativeMethods.FetchFields(taosRes);
                    List<object> actualResData = Tools.ReadRawBlock(pdata, actualMeta, numOfRows);

                    //Assert Meta data
                    _output.WriteLine("Assert Meta data");
                    for (int i = 0; i < actualMeta.Count; i++)
                    {
                        Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                        Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                        Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
                    }
                    // Assert retrieve data
                    _output.WriteLine("Assert retrieve data");

                    for (int i = 0; i < actualResData.Count; i++)
                    {
                        //_output.WriteLine("{0},{1},{2}", i, expectResData[i], actualResData[i]);
                        Assert.Equal(expectResData[i], actualResData[i]);
                    }

                    NativeMethods.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
                }
                else
                {
                    if (numOfRows == 0)
                    {
                        _output.WriteLine("async retrieve complete.");

                    }
                    else
                    {
                        _output.WriteLine($"FetchRowAsync callback error, error code {numOfRows}");
                    }
                    NativeMethods.FreeResult(taosRes);
                }
            }
        }

        /// <author>xiaolei</author>
        /// <Name>QueryAsync.JSONTag</Name>
        /// <describe>Test query_a with table which has JSON tags.</describe>
        /// <filename>QueryAsync.cs</filename>
        /// <result>pass or failed </result> 
        [Fact(DisplayName = "QueryAsync.JSONTag()"), TestExeOrder(3), Trait("Category", "JSONTag")]
        public void JSONTag()
        {
            IntPtr conn = database.Conn;
            IntPtr _res = IntPtr.Zero;

            var tableName = "query_a_tj";
            string createSql = Tools.CreateTable(tableName, true, true);
            List<object> coldata = Tools.ColumnsList(5);
            List<object> tags = Tools.TagsList(1, true);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            List<object> expectResData = Tools.ConstructResData(coldata, tags, 5);
            string insertSql = Tools.ConstructInsertSql($"{tableName}_j1", tableName, coldata, tags, 5);
            string selectSql = $"select * from {tableName}";

            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            QueryAsyncCallback fq = new QueryAsyncCallback(QueryCallback);
            NativeMethods.QueryAsync(conn, selectSql, fq, IntPtr.Zero);
            Thread.Sleep(2000);

            void QueryCallback(IntPtr param, IntPtr taosRes, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                    NativeMethods.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
                }
                else
                {
                    _output.WriteLine($"async query data failed, failed code {code}");
                }

            }

            void FetchRawBlockCallback(IntPtr param, IntPtr taosRes, int numOfRows)
            {
                if (numOfRows > 0)
                {

                    IntPtr pdata = NativeMethods.GetRawBlock(taosRes);
                    List<TDengineMeta> actualMeta = NativeMethods.FetchFields(taosRes);
                    List<object> actualResData = Tools.ReadRawBlock(pdata, actualMeta, numOfRows);

                    //Assert Meta data
                    _output.WriteLine("Assert Meta data");
                    for (int i = 0; i < actualMeta.Count; i++)
                    {
                        Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                        Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                        Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
                    }
                    // Assert retrieve data
                    _output.WriteLine("Assert retrieve data");

                    for (int i = 0; i < actualResData.Count; i++)
                    {
                        //_output.WriteLine("{0},{1},{2}", i, expectResData[i], actualResData[i]);
                        Assert.Equal(expectResData[i], actualResData[i]);
                    }

                    NativeMethods.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
                }
                else
                {
                    if (numOfRows == 0)
                    {
                        _output.WriteLine("async retrieve complete.");

                    }
                    else
                    {
                        _output.WriteLine($"FetchRowAsync callback error, error code {numOfRows}");
                    }
                    NativeMethods.FreeResult(taosRes);
                }
            }
        }
        
        [Fact(DisplayName = "QueryAsync.ReqId()"), TestExeOrder(4), Trait("Category", "ReqId")]
        public void ReqId()
        {
            IntPtr conn = database.Conn;

            var tableName = "query_a_with_reqid";
            string createSql = Tools.CreateTable(tableName, false, false);
            List<object> coldata = Tools.ColumnsList(5);
            List<TDengineMeta> expectResMeta = Tools.GetMetaFromDDL(createSql);
            string insertSql = Tools.ConstructInsertSql(tableName, "", coldata, null, 5);
            string selectSql = $"select * from {tableName}";

            Tools.ExecuteUpdate(conn, createSql, _output);
            Tools.ExecuteUpdate(conn, insertSql, _output);

            QueryAsyncCallback fq = new QueryAsyncCallback(QueryCallback);
            NativeMethods.QueryAsyncWithReqid(conn, selectSql, fq, IntPtr.Zero,TDengine.Driver.ReqId.GetReqId());
            Thread.Sleep(2000);

            void QueryCallback(IntPtr param, IntPtr taosRes, int code)
            {
                if (code == 0 && taosRes != IntPtr.Zero)
                {
                    FetchRawBlockAsyncCallback fetchRowAsyncCallback = new FetchRawBlockAsyncCallback(FetchRawBlockCallback);
                    NativeMethods.FetchRawBlockAsync(taosRes, fetchRowAsyncCallback, param);
                }
                else
                {
                    _output.WriteLine($"async query data failed, failed code {code}");
                }

            }

            void FetchRawBlockCallback(IntPtr param, IntPtr taosRes, int numOfRows)
            {
                if (numOfRows > 0)
                {

                    IntPtr pdata = NativeMethods.GetRawBlock(taosRes);
                    List<TDengineMeta> actualMeta = NativeMethods.FetchFields(taosRes);
                    List<object> actualResData = Tools.ReadRawBlock(pdata, actualMeta, numOfRows);

                    //Assert Meta data
                    _output.WriteLine("Assert Meta data");
                    for (int i = 0; i < actualMeta.Count; i++)
                    {
                        Assert.Equal(expectResMeta[i].name, actualMeta[i].name);
                        Assert.Equal(expectResMeta[i].type, actualMeta[i].type);
                        Assert.Equal(expectResMeta[i].size, actualMeta[i].size);
                    }
                    // Assert retrieve data
                    _output.WriteLine("Assert retrieve data");

                    for (int i = 0; i < actualResData.Count; i++)
                    {
                        //_output.WriteLine("{0},{1},{2}", i, coldata[i], actualResData[i]);
                        Assert.Equal(coldata[i], actualResData[i]);
                    }

                    NativeMethods.FetchRawBlockAsync(taosRes, FetchRawBlockCallback, param);
                }
                else
                {
                    if (numOfRows == 0)
                    {
                        _output.WriteLine("async retrieve complete.");

                    }
                    else
                    {
                        _output.WriteLine($"FetchRowAsync callback error, error code {numOfRows}");
                    }
                    NativeMethods.FreeResult(taosRes);
                }
            }

        }


    }
}
