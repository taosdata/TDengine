using System;
using System.Text;
using TDengine.Driver;
using TDengine.Driver.Client;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Client.Query
{
    public partial class Client
    {
        private readonly ITestOutputHelper _output;
        private readonly string _nativeConnectString;
        private readonly string _wsConnectString;
        private readonly string _cloudConnectString;

        public Client(ITestOutputHelper output)
        {
            this._output = output;
            this._nativeConnectString = "host=localhost;port=6030;username=root;password=taosdata";
            this._wsConnectString =
                "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata;enableCompression=true";
            var cloudHost = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_ENDPOINT");
            var cloudToken = Environment.GetEnvironmentVariable("TDENGINE_CLOUD_TOKEN");
            if (!string.IsNullOrEmpty(cloudHost) && !string.IsNullOrEmpty(cloudToken))
            {
                this._cloudConnectString = GetCloudConnectString(cloudHost, cloudToken);
            }
        }

        private static string GetCloudConnectString(string host, string token)
        {
            return
                $"protocol=WebSocket;host={host};port=443;useSSL=true;token={token};enableCompression=true";
        }

        private object[][] GenerateValue(TDenginePrecision precision, out string sql)
        {
            Random rand = new Random();
            bool v1 = true;
            sbyte v2 = (sbyte)rand.Next(sbyte.MinValue, sbyte.MaxValue);
            short v3 = (short)rand.Next(short.MinValue, short.MaxValue);
            int v4 = rand.Next();
            long v5 = rand.Next();
            byte v6 = (byte)rand.Next(byte.MinValue, byte.MaxValue);
            ushort v7 = (ushort)rand.Next(ushort.MinValue, ushort.MaxValue);
            uint v8 = (uint)rand.Next();
            ulong v9 = (ulong)rand.Next();
            float v10 = (float)rand.NextDouble();
            double v11 = rand.NextDouble();
            var dateTime = DateTime.Now;
            long ts = 0;
            long nextSecond = 0;
            switch (precision)
            {
                case TDenginePrecision.TSDB_TIME_PRECISION_MILLI:
                    ts = (dateTime.ToUniversalTime().Ticks - TDengineConstant.TimeZero.Ticks) / 10000;
                    nextSecond = (dateTime.Add(TimeSpan.FromSeconds(1)).ToUniversalTime().Ticks -
                                  TDengineConstant.TimeZero.Ticks) / 10000;
                    break;
                case TDenginePrecision.TSDB_TIME_PRECISION_NANO:
                    ts = (dateTime.ToUniversalTime().Ticks - TDengineConstant.TimeZero.Ticks) * 100;
                    nextSecond = (dateTime.Add(TimeSpan.FromSeconds(1)).ToUniversalTime().Ticks -
                                  TDengineConstant.TimeZero.Ticks) * 100;
                    break;
                case TDenginePrecision.TSDB_TIME_PRECISION_MICRO:
                    ts = (dateTime.ToUniversalTime().Ticks - TDengineConstant.TimeZero.Ticks) / 10;
                    nextSecond = (dateTime.Add(TimeSpan.FromSeconds(1)).ToUniversalTime().Ticks -
                                  TDengineConstant.TimeZero.Ticks) / 10;
                    break;
            }

            sql = string.Format(
                "values({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},'test_binary','test_nchar','中文','POINT(100 100)')({12},null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)",
                ts,
                v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11,
                nextSecond);
            return new object[][]
            {
                new object[]
                {
                    TDengineConstant.ConvertTimeToDatetime(ts, precision), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11,
                    Encoding.UTF8.GetBytes("test_binary"),
                    "test_nchar", Encoding.UTF8.GetBytes("中文"),
                    new byte[]
                    {
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                    }
                },
                new object[]
                {
                    TDengineConstant.ConvertTimeToDatetime(nextSecond, precision), null, null, null, null, null, null,
                    null, null, null, null, null, null, null, null, null
                }
            };
        }

        private static string GenerateCreateTableSql(string tableName)
        {
            var createTableSql = $"create table if not exists {tableName} (ts timestamp," +
                                 "c1 bool," +
                                 "c2 tinyint," +
                                 "c3 smallint," +
                                 "c4 int," +
                                 "c5 bigint," +
                                 "c6 tinyint unsigned," +
                                 "c7 smallint unsigned," +
                                 "c8 int unsigned," +
                                 "c9 bigint unsigned," +
                                 "c10 float," +
                                 "c11 double," +
                                 "c12 binary(20)," +
                                 "c13 nchar(20)," +
                                 "c14 varbinary(20)," +
                                 "c15 geometry(100)" +
                                 ")" +
                                 "tags(t json)";
            return createTableSql;
        }

        private static Array[] TransposeToTypedArrays(object[][] data)
        {
            var aTs = new DateTime[] { (DateTime)data[0][0], (DateTime)data[1][0] };
            var a1 = new bool?[] { (bool)data[0][1], (bool?)data[1][1], };
            var a2 = new sbyte?[] { (sbyte)data[0][2], (sbyte?)data[1][2] };
            var a3 = new short?[] { (short)data[0][3], (short?)data[1][3] };
            var a4 = new int?[] { (int)data[0][4], (int?)data[1][4] };
            var a5 = new long?[] { (long)data[0][5], (long?)data[1][5] };
            var a6 = new byte?[] { (byte)data[0][6], (byte?)data[1][6] };
            var a7 = new ushort?[] { (ushort)data[0][7], (ushort?)data[1][7] };
            var a8 = new uint?[] { (uint)data[0][8], (uint?)data[1][8] };
            var a9 = new ulong?[] { (ulong)data[0][9], (ulong?)data[1][9] };
            var a10 = new float?[] { (float)data[0][10], (float?)data[1][10] };
            var a11 = new double?[] { (double)data[0][11], (double?)data[1][11] };
            var aBinary = new byte[][] { (byte[])data[0][12], (byte[])data[1][12] };
            var aNchar = new string[] { (string)data[0][13], (string)data[1][13] };
            var aVarBinary = new byte[][] { (byte[])data[0][14], (byte[])data[1][14] };
            var aGeometry = new byte[][] { (byte[])data[0][15], (byte[])data[1][15] };
            return new Array[]
                { aTs, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, aBinary, aNchar, aVarBinary, aGeometry };
        }

        private string PrecisionString(TDenginePrecision precision)
        {
            switch (precision)
            {
                case TDenginePrecision.TSDB_TIME_PRECISION_NANO:
                    return "ns";
                case TDenginePrecision.TSDB_TIME_PRECISION_MICRO:
                    return "us";
                case TDenginePrecision.TSDB_TIME_PRECISION_MILLI:
                    return "ms";
            }

            return "ms";
        }

        private static bool IsCloudTest(ConnectionStringBuilder builder)
        {
            return !string.IsNullOrEmpty(builder.Token);
        }

        private void QueryTest(string connectString, string db, TDenginePrecision precision)
        {
            var data = this.GenerateValue(precision, out var insertSql);
            var builder = new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                var now = DateTime.Now;
                var superTableName = $"all_type_stb_{now.Ticks}";
                var subTableName = $"all_type_ctb_{now.Ticks}";
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                        client.Exec($"create database {db} precision '{PrecisionString(precision)}'");
                    }

                    client.Exec($"use {db}");
                    var createTableSql = GenerateCreateTableSql(superTableName);
                    client.Exec(createTableSql);
                    string insertQuery =
                        $"insert into {subTableName} using {superTableName} tags('{{\"a\":\"b\"}}') {insertSql}";
                    client.Exec(insertQuery);
                    string query = $"select * from {superTableName} order by ts asc";
                    using (var rows = client.Query(query))
                    {
                        this.AssertColumn(rows);
                        this.AssertValue(rows, data);
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop table if exists {superTableName}");
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                    }
                }
            }
        }

        private void QueryWithReqIDTest(string connectString, string db, TDenginePrecision precision)
        {
            var data = this.GenerateValue(precision, out var insertSql);
            var builder = new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                var now = DateTime.Now;
                var superTableName = $"all_type_stb_{now.Ticks}";
                var subTableName = $"all_type_ctb_{now.Ticks}";
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                        client.Exec($"create database {db} precision '{PrecisionString(precision)}'", ReqId.GetReqId());
                    }

                    client.Exec($"use {db}", ReqId.GetReqId());
                    string createTableSql = GenerateCreateTableSql(superTableName);
                    client.Exec(createTableSql, ReqId.GetReqId());
                    string insertQuery =
                        $"insert into {subTableName} using {superTableName} tags('{{\"a\":\"b\"}}') {insertSql}";
                    client.Exec(insertQuery, ReqId.GetReqId());
                    string query = $"select * from {superTableName} order by ts asc";
                    using (var rows = client.Query(query, ReqId.GetReqId()))
                    {
                        this.AssertColumn(rows);
                        this.AssertValue(rows, data);
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop table if exists {superTableName}", ReqId.GetReqId());
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                    }
                }
            }
        }


        private void StmtTest(string connectString, string db, TDenginePrecision precision)
        {
            var data = this.GenerateValue(precision, out _);
            var builder = new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                var now = DateTime.Now;
                var superTableName = $"all_type_stb_{now.Ticks}";
                var subTableName = $"all_type_ctb_{now.Ticks}";
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                        client.Exec($"create database {db} precision '{PrecisionString(precision)}'");
                    }

                    client.Exec($"use {db}");
                    var createTableSql = GenerateCreateTableSql(superTableName);
                    client.Exec(createTableSql);
                    var stmt = client.StmtInit();
                    StringBuilder questionMarks = new StringBuilder();
                    var count = data[0].Length;
                    for (int i = 0; i < count; i++)
                    {
                        questionMarks.Append("?");
                        if (i < count - 1)
                        {
                            questionMarks.Append(", ");
                        }
                    }

                    var values = questionMarks.ToString();
                    stmt.Prepare($"insert into ? using {superTableName} tags(?) values({values})");
                    var isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName(subTableName);
                    stmt.SetTags(new object[] { "{\"a\":\"b\"}" });
                    stmt.BindRow(data[0]);
                    stmt.BindRow(data[1]);
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)2, affected);
                    stmt.Prepare($"select * from {superTableName} where ts >= ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[] { data[0][0] });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        this.AssertColumn(rows);
                        this.AssertValue(rows, data);
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop table if exists {superTableName}");
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                    }
                }
            }
        }


        private void StmtWithReqIDTest(string connectString, string db, TDenginePrecision precision)
        {
            var data = this.GenerateValue(precision, out _);
            var builder = new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                var now = DateTime.Now;
                var superTableName = $"all_type_stb_{now.Ticks}";
                var subTableName = $"all_type_ctb_{now.Ticks}";
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                        client.Exec($"create database {db} precision '{PrecisionString(precision)}'", ReqId.GetReqId());
                    }

                    client.Exec($"use {db}", ReqId.GetReqId());
                    var createTableSql = GenerateCreateTableSql(superTableName);
                    client.Exec(createTableSql, ReqId.GetReqId());
                    var stmt = client.StmtInit(ReqId.GetReqId());
                    StringBuilder questionMarks = new StringBuilder();
                    var count = data[0].Length;
                    for (int i = 0; i < count; i++)
                    {
                        questionMarks.Append("?");
                        if (i < count - 1)
                        {
                            questionMarks.Append(", ");
                        }
                    }

                    var values = questionMarks.ToString();
                    stmt.Prepare($"insert into ? using {superTableName} tags(?) values({values})");
                    var isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName(subTableName);
                    stmt.SetTags(new object[] { "{\"a\":\"b\"}" });
                    stmt.BindRow(data[0]);
                    stmt.BindRow(data[1]);
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)2, affected);
                    stmt.Prepare($"select * from {superTableName} where ts >= ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[] { data[0][0] });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        this.AssertColumn(rows);
                        this.AssertValue(rows, data);
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop table if exists {superTableName}", ReqId.GetReqId());
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                    }
                }
            }
        }


        private void StmtBindColumnsTest(string connectString, string db, TDenginePrecision precision)
        {
            var data = this.GenerateValue(precision, out _);
            var transposedData = TransposeToTypedArrays(data);

            var builder =
                new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                var now = DateTime.Now;
                var superTableName = $"all_type_stb_{now.Ticks}";
                var subTableName = $"all_type_ctb_{now.Ticks}";
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                        client.Exec($"create database {db} precision '{PrecisionString(precision)}'", ReqId.GetReqId());
                    }

                    client.Exec($"use {db}");
                    var createTableSql = GenerateCreateTableSql(superTableName);
                    client.Exec(createTableSql);
                    var stmt = client.StmtInit(ReqId.GetReqId());
                    stmt.Prepare(
                        $"insert into ? using {superTableName} tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                    var isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName(subTableName);
                    stmt.SetTags(new object[] { "{\"a\":\"b\"}" });
                    var fields = stmt.GetColFields();
                    stmt.BindColumn(fields, transposedData);
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)2, affected);
                    stmt.Prepare($"select * from {superTableName} where ts >= ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[] { data[0][0] });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var result = stmt.Result())
                    {
                        this.AssertColumn(result);
                        this.AssertValue(result, data);
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop table if exists {superTableName}");
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                    }
                }
            }
        }


        private void VarbinaryTest(string connectString, string db)
        {
            DateTime dateTime = DateTime.Now;
            var ts = (dateTime.ToUniversalTime().Ticks - TDengineConstant.TimeZero.Ticks) / 10000;
            var now = TDengineConstant.ConvertTimeToDatetime(ts, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
            var builder =
                new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                var tableName = $"test_varbinary_{dateTime.Ticks}";
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                        client.Exec($"create database {db} precision 'ms'");
                    }

                    client.Exec($"use {db}");
                    client.Exec($"create table if not exists {tableName}(ts timestamp,c1 varbinary(65517))");
                    var stmt = client.StmtInit(ReqId.GetReqId());
                    stmt.Prepare($"insert into {tableName} values(?,?)");
                    var isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    var fields = stmt.GetColFields();
                    var size = 65517;
                    var data = new byte[size];
                    for (int i = 0; i < size; i++)
                    {
                        data[i] = (byte)'a';
                    }

                    stmt.BindColumn(fields, new DateTime[] { now }, new byte[][] { data });
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)1, affected);
                    stmt.Prepare($"select * from {tableName} where c1 = ?");
                    stmt.BindRow(new object[] { data });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var resut = stmt.Result())
                    {
                        var haveNext = resut.Read();
                        Assert.True(haveNext);
                        Assert.Equal(now, resut.GetValue(0));
                        Assert.Equal(data, resut.GetValue(1));
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop table if exists {tableName}");
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                    }
                }
            }
        }


        private void InfluxDBTest(string connectString, string db)
        {
            var builder =
                new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                        client.Exec($"create database {db} precision 'ns'");
                    }

                    client.Exec($"use {db}");
                    var data =
                        @"http_response,host=host161,method=GET,result=success,server=http://localhost,status_code=404 response_time=0.003226372,http_response_code=404i,content_length=19i,result_type=""success"",result_code=0i 1648090640000000000
request_histogram_latency_seconds_max,aaa=bb,api_range=all,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
process_files_max_files,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=10240 1648090640000000000
request_timer_seconds,host=host161,quantile=0.5,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.9,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000 
request_timer_seconds,host=host161,quantile=0.95,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,quantile=0.99,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
request_timer_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus 0.223696211=0,0.016777216=0,0.178956969=0,0.156587348=0,0.2=0,0.626349396=0,0.015379112=0,5=0,0.089478485=0,0.357913941=0,5.726623061=0,0.008388607=0,0.894784851=0,0.006990506=0,3.937053352=0,0.001=0,0.061516456=0,0.134217727=0,1.431655765=0,0.005592405=0,0.984263336=0,0.001398101=0,3.22122547=0,0.033554431=0,0.805306366=0,0.002446676=0,0.003844776=0,0.20132659=0,1.073741824=0,0.022369621=0,1=0,0.002796201=0,1.789569706=0,0.001048576=0,0.246065832=0,0.050331646=0,4.294967296=0,8.589934591=0,0.536870911=0,0.447392426=0,2.505397588=0,10=0,0.013981011=0,0.003495251=0,0.044739241=0,2.863311529=0,0.039146836=0,0.268435456=0,sum=0,3.579139411=0,7.158278826=0,0.011184809=0,0.01258291=0,0.1=0,0.003145726=0,0.055924051=0,0.067108864=0,0.004194304=0,0.001747626=0,0.002097151=0,2.147483647=0,count=0,0.715827881=0,0.009786708=0,0.111848106=0,0.027962026=0,+Inf=0 1648090640000000000
executor_completed_tasks_total,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4718592 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=100139008 1648090640000000000
jvm_memory_committed_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=123207680 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=44998656 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=8847360 1648090640000000000
jvm_memory_committed_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=6463488 1648090640000000000
executor_active_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_active_max_sessions,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
system_cpu_count,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=8 1648090640000000000
logback_events_total,host=host161,level=warn,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=debug,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=error,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=trace,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
logback_events_total,host=host161,level=info,url=http://192.168.17.148:8080/actuator/prometheus counter=7 1648090640000000000
application_ready_time_seconds,host=host161,main_application_class=cn.iospider.actuatormicrometer.ActuatorMicrometerApplication,url=http://192.168.17.148:8080/actuator/prometheus gauge=28.542 1648090640000000000
jvm_buffer_total_capacity_bytes,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=57345 1648090640000000000
jvm_buffer_total_capacity_bytes,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_live_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=41 1648090640000000000
jvm_gc_max_data_size_bytes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=2863661056 1648090640000000000
executor_pool_max_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=2147483647 1648090640000000000
jvm_gc_overhead_percent,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.00010333333333333333 1648090640000000000
http_server_requests_seconds_max,exception=None,host=host161,method=GET,outcome=SUCCESS,status=200,uri=/actuator/prometheus,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.008994315 1648090640000000000
http_server_requests_seconds_max,exception=None,host=host161,method=GET,outcome=CLIENT_ERROR,status=404,uri=/**,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_rejected_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
request_histogram_latency_seconds,aaa=bb,api_range=all,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=0,sum=0 1648090640000000000
disk_free_bytes,host=host161,path=/Users/jtlian/Downloads/actuator-micrometer/.,url=http://192.168.17.148:8080/actuator/prometheus gauge=77683585024 1648090640000000000
process_cpu_usage,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.0005609754336738071 1648090640000000000
jvm_threads_peak_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=42 1648090640000000000
jvm_gc_memory_allocated_bytes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=271541440 1648090640000000000
jvm_gc_live_data_size_bytes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=14251648 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4565576 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=14268032 1648090640000000000
jvm_memory_used_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=16630104 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=41165008 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=8792832 1648090640000000000
jvm_memory_used_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=5735248 1648090640000000000
jvm_buffer_count_buffers,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=9 1648090640000000000
jvm_buffer_count_buffers,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
application_started_time_seconds,host=host161,main_application_class=cn.iospider.actuatormicrometer.ActuatorMicrometerApplication,url=http://192.168.17.148:8080/actuator/prometheus gauge=28.535 1648090640000000000
process_start_time_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=1648087193.449 1648090640000000000
jvm_memory_usage_after_gc_percent,area=heap,host=host161,pool=long-lived,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.004982444402805749 1648090640000000000
system_cpu_usage,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0.11106101593026751 1648090640000000000
tomcat_sessions_active_current_sessions,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
executor_queue_remaining_tasks,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=2147483647 1648090640000000000
jvm_threads_daemon_threads,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=37 1648090640000000000
process_uptime_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=3446.817 1648090640000000000
tomcat_sessions_alive_max_seconds,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
executor_queued_tasks,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
request_timer_seconds_max,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
tomcat_sessions_created_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=runnable,url=http://192.168.17.148:8080/actuator/prometheus gauge=17 1648090640000000000
jvm_threads_states_threads,host=host161,state=blocked,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=waiting,url=http://192.168.17.148:8080/actuator/prometheus gauge=19 1648090640000000000
jvm_threads_states_threads,host=host161,state=timed-waiting,url=http://192.168.17.148:8080/actuator/prometheus gauge=5 1648090640000000000
jvm_threads_states_threads,host=host161,state=new,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_threads_states_threads,host=host161,state=terminated,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
process_files_open_files,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=119 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Survivor\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=4718592 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Old\ Gen,url=http://192.168.17.148:8080/actuator/prometheus gauge=2863661056 1648090640000000000
jvm_memory_max_bytes,area=heap,host=host161,id=PS\ Eden\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=1411907584 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Metaspace,url=http://192.168.17.148:8080/actuator/prometheus gauge=-1 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Code\ Cache,url=http://192.168.17.148:8080/actuator/prometheus gauge=251658240 1648090640000000000
jvm_memory_max_bytes,area=nonheap,host=host161,id=Compressed\ Class\ Space,url=http://192.168.17.148:8080/actuator/prometheus gauge=1073741824 1648090640000000000
executor_pool_size_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
disk_total_bytes,host=host161,path=/Users/jtlian/Downloads/actuator-micrometer/.,url=http://192.168.17.148:8080/actuator/prometheus gauge=328000839680 1648090640000000000
http_server_requests_seconds,exception=None,host=host161,method=GET,outcome=SUCCESS,status=200,uri=/actuator/prometheus,url=http://192.168.17.148:8080/actuator/prometheus count=7,sum=0.120204066 1648090640000000000
http_server_requests_seconds,exception=None,host=host161,method=GET,outcome=CLIENT_ERROR,status=404,uri=/**,url=http://192.168.17.148:8080/actuator/prometheus count=4,sum=0.019408184 1648090640000000000
jvm_buffer_memory_used_bytes,host=host161,id=direct,url=http://192.168.17.148:8080/actuator/prometheus gauge=57346 1648090640000000000
jvm_buffer_memory_used_bytes,host=host161,id=mapped,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_memory_promoted_bytes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=3055728 1648090640000000000
jvm_classes_loaded_classes,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=8526 1648090640000000000
system_load_average_1m,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=3.10107421875 1648090640000000000
tomcat_sessions_expired_sessions_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
executor_pool_core_threads,host=host161,name=applicationTaskExecutor,url=http://192.168.17.148:8080/actuator/prometheus gauge=8 1648090640000000000
jvm_classes_unloaded_classes_total,host=host161,url=http://192.168.17.148:8080/actuator/prometheus counter=0 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ major\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=1,sum=0.037 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ minor\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=1,sum=0.005 1648090640000000000
jvm_gc_pause_seconds,action=end\ of\ minor\ GC,cause=Allocation\ Failure,host=host161,url=http://192.168.17.148:8080/actuator/prometheus count=2,sum=0.041 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ major\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ minor\ GC,cause=Metadata\ GC\ Threshold,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000
jvm_gc_pause_seconds_max,action=end\ of\ minor\ GC,cause=Allocation\ Failure,host=host161,url=http://192.168.17.148:8080/actuator/prometheus gauge=0 1648090640000000000";
                    client.SchemalessInsert(new string[] { data }, TDengineSchemalessProtocol.TSDB_SML_LINE_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS, 0, ReqId.GetReqId());
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                    }
                }
            }
        }

        private void TelnetTest(string connectString, string db)
        {
            var builder =
                new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                        client.Exec($"create database {db} precision 'ns'");
                    }

                    client.Exec($"use {db}");
                    var data = new string[]
                    {
                        "sys_if_bytes_out 1479496100 1.3E3 host=web01 interface=eth0",
                        "sys_procs_running 1479496100 42 host=web01",
                    };
                    client.SchemalessInsert(data, TDengineSchemalessProtocol.TSDB_SML_TELNET_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                    }
                }
            }
        }

        private void SMLJsonTest(string connectString, string db)
        {
            var builder =
                new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                        client.Exec($"create database {db} precision 'ns'");
                    }

                    client.Exec($"use {db}");
                    var data = new string[]
                    {
                        @"{
    ""metric"": ""sys"",
    ""timestamp"": 1692346407,
    ""value"": 18,
    ""tags"": {
       ""host"": ""web01"",
       ""dc"": ""lga""
    }
}"
                    };
                    client.SchemalessInsert(data, TDengineSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,
                        TDengineSchemalessPrecision.TSDB_SML_TIMESTAMP_NOT_CONFIGURED, 0, ReqId.GetReqId());
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}");
                    }
                }
            }
        }

        private void AssertColumn(IRows result)
        {
            Assert.Equal(1, result.GetOrdinal("c1"));
            var fieldCount = result.FieldCount;
            Assert.Equal(17, fieldCount);
            Assert.Equal("ts", result.GetName(0));
            Assert.Equal("c1", result.GetName(1));
            Assert.Equal("c2", result.GetName(2));
            Assert.Equal("c3", result.GetName(3));
            Assert.Equal("c4", result.GetName(4));
            Assert.Equal("c5", result.GetName(5));
            Assert.Equal("c6", result.GetName(6));
            Assert.Equal("c7", result.GetName(7));
            Assert.Equal("c8", result.GetName(8));
            Assert.Equal("c9", result.GetName(9));
            Assert.Equal("c10", result.GetName(10));
            Assert.Equal("c11", result.GetName(11));
            Assert.Equal("c12", result.GetName(12));
            Assert.Equal("c13", result.GetName(13));
            Assert.Equal("c14", result.GetName(14));
            Assert.Equal("c15", result.GetName(15));
            Assert.Equal("t", result.GetName(16));
            Assert.Equal(-1, result.AffectRows);
        }

        private void AssertValue(IRows rows, object[][] data)
        {
            for (int i = 0; i < data.Length; i++)
            {
                var haveNext = rows.Read();
                Assert.True(haveNext);
                for (int j = 0; j < data[i].Length; j++)
                {
                    // this._output.WriteLine($"{data[i][j]}:{rows.GetValue(j)}");
                    var val = rows.GetValue(j);
                    var expectVal = data[i][j];
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

                Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(data[i].Length));
            }
        }

        private void QueryConcurrencyTest(string connectString, string db)
        {
            var precision = TDenginePrecision.TSDB_TIME_PRECISION_MILLI;
            var builder = new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            var client = DbDriver.Open(builder);
            var count = 30;
            var tableName = $"test_concurrency_{DateTime.Now.Ticks}";
            try
            {
                if (!inCloud)
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db} precision '{PrecisionString(precision)}'");
                }

                client.Exec($"use {db}");
                client.Exec($"create table if not exists {tableName} (ts timestamp, a int, b float, c binary(10))");
                var ts = new long[count];
                var dateTime = DateTime.Now;
                var tsv = new DateTime[count];
                for (int i = 0; i < count; i++)
                {
                    ts[i] = (dateTime.Add(TimeSpan.FromSeconds(i)).ToUniversalTime().Ticks -
                             TDengineConstant.TimeZero.Ticks) / 10000;
                    tsv[i] = TDengineConstant.ConvertTimeToDatetime(ts[i], precision);
                }

                var valuesStr = "";
                for (int i = 0; i < count; i++)
                {
                    valuesStr += $"({ts[i]}, {i}, {i}, '中文')";
                }

                client.Exec($"insert into {tableName} values {valuesStr}");
                var tasks = new System.Collections.Generic.List<System.Threading.Tasks.Task>();
                for (var i = 0; i < count; i++)
                {
                    int localI = i;
                    string query = $"select * from {tableName} where ts = " + ts[localI];
                    tasks.Add(System.Threading.Tasks.Task.Run(() =>
                    {
                        using (var rows = client.Query(query))
                        {
                            Assert.Equal(1, rows.GetOrdinal("a"));
                            var fieldCount = rows.FieldCount;
                            Assert.Equal(4, fieldCount);
                            Assert.Equal("ts", rows.GetName(0));
                            Assert.Equal("a", rows.GetName(1));
                            Assert.Equal("b", rows.GetName(2));
                            Assert.Equal("c", rows.GetName(3));
                            var haveNext = rows.Read();
                            Assert.True(haveNext);
                            Assert.Equal(tsv[localI], rows.GetValue(0));
                            Assert.Equal(localI, rows.GetValue(1));
                            Assert.Equal((float)localI, (float)rows.GetValue(2), 7);
                            Assert.Equal(Encoding.UTF8.GetBytes("中文"), rows.GetValue(3));
                        }
                    }));
                }

                System.Threading.Tasks.Task.WaitAll(tasks.ToArray());
            }
            catch (Exception e)
            {
                _output.WriteLine(e.ToString());
                throw;
            }
            finally
            {
                client.Exec($"drop table if exists {tableName}");
                if (!inCloud)
                {
                    client.Exec($"drop database if exists {db}");
                }

                client.Dispose();
            }
        }
    }
}