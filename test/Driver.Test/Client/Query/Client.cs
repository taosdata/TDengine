using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using System.Threading.Tasks;
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

        private static Decimal GenerateDecimal(int precision, int scale)
        {
            var random = new Random();
            var sb = new StringBuilder();

            int integerDigits = precision - scale;

            sb.Append(random.Next(1, 10));

            for (int i = 1; i < integerDigits; i++)
            {
                sb.Append(random.Next(0, 10));
            }

            if (scale > 0)
            {
                sb.Append('.');
                for (int i = 0; i < scale; i++)
                {
                    sb.Append(random.Next(0, 10));
                }
            }

            return decimal.Parse(sb.ToString());
        }

        private object[][] GenerateValue(TDenginePrecision precision, bool withDecimal, out string sql,
            TimeZoneInfo tz = null)
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
            decimal v16 = GenerateDecimal(20, 4);
            decimal v17 = GenerateDecimal(8, 4);

            bool v1_3 = false;
            sbyte v2_3 = sbyte.MinValue;
            short v3_3 = short.MinValue;
            int v4_3 = int.MinValue;
            long v5_3 = long.MinValue;
            byte v6_3 = byte.MaxValue;
            ushort v7_3 = ushort.MaxValue;
            uint v8_3 = uint.MaxValue;
            ulong v9_3 = ulong.MaxValue;
            float v10_3 = (float)rand.NextDouble();
            double v11_3 = rand.NextDouble();
            decimal v16_3 = decimal.Parse("9999999999999999.9999");
            decimal v17_3 = decimal.Parse("9999.9999");

            bool v1_4 = true;
            sbyte v2_4 = sbyte.MaxValue;
            short v3_4 = short.MaxValue;
            int v4_4 = int.MaxValue;
            long v5_4 = long.MaxValue;
            byte v6_4 = byte.MaxValue;
            ushort v7_4 = ushort.MaxValue;
            uint v8_4 = uint.MaxValue;
            ulong v9_4 = ulong.MaxValue;
            float v10_4 = (float)rand.NextDouble();
            double v11_4 = rand.NextDouble();
            decimal v16_4 = decimal.Parse("0.9999");
            decimal v17_4 = decimal.Parse("0.9999");

            var rowCount = 5;
            var dateTime = DateTime.Now;
            var timeStampes = new long[rowCount];
            switch (precision)
            {
                case TDenginePrecision.TSDB_TIME_PRECISION_MILLI:
                    for (int i = 0; i < rowCount; i++)
                    {
                        timeStampes[i] = (dateTime.Add(TimeSpan.FromSeconds(i)).ToUniversalTime().Ticks -
                                          TDengineConstant.TimeZero.Ticks) / 10000;
                    }

                    break;
                case TDenginePrecision.TSDB_TIME_PRECISION_NANO:
                    for (int i = 0; i < rowCount; i++)
                    {
                        timeStampes[i] = (dateTime.Add(TimeSpan.FromSeconds(i)).ToUniversalTime().Ticks -
                                          TDengineConstant.TimeZero.Ticks) * 100;
                    }

                    break;
                case TDenginePrecision.TSDB_TIME_PRECISION_MICRO:
                    for (int i = 0; i < rowCount; i++)
                    {
                        timeStampes[i] = (dateTime.Add(TimeSpan.FromSeconds(i)).ToUniversalTime().Ticks -
                                          TDengineConstant.TimeZero.Ticks) / 10;
                    }

                    break;
            }

            if (withDecimal)
            {
                sql = $"values" +
                      $"({timeStampes[0]},{v1},{v2},{v3},{v4},{v5},{v6},{v7},{v8},{v9},{v10:G9},{v11:G17},'test_binary','test_nchar','中文','POINT(100 100)',{v16},{v17})" +
                      $"({timeStampes[1]},null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)" +
                      $"({timeStampes[2]},{v1},{v2},{v3},{v4},{v5},{v6},{v7},{v8},{v9},{v10},{v11},'中文','中文','中文','POINT(100 100)',{v16},{v17})" +
                      $"({timeStampes[3]},{v1_3},{v2_3},{v3_3},{v4_3},{v5_3},{v6_3},{v7_3},{v8_3},{v9_3},{v10_3},{v11_3},'中文','中文','中文','POINT(100 100)',{v16_3},{v17_3})" +
                      $"({timeStampes[4]},{v1_4},{v2_4},{v3_4},{v4_4},{v5_4},{v6_4},{v7_4},{v8_4},{v9_4},{v10_4},{v11_4},'中文','中文','中文','POINT(100 100)',{v16_4},{v17_4})";
                return new object[][]
                {
                    new object[]
                    {
                        TDengineConstant.ConvertTimestampToDateTime(timeStampes[0], precision), v1, v2, v3, v4, v5, v6,
                        v7,
                        v8, v9, v10,
                        v11,
                        Encoding.UTF8.GetBytes("test_binary"),
                        "test_nchar", Encoding.UTF8.GetBytes("中文"),
                        new byte[]
                        {
                            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                        },
                        v16, v17,
                    },
                    new object[]
                    {
                        TDengineConstant.ConvertTimestampToDateTime(timeStampes[1], precision), null, null, null, null,
                        null,
                        null,
                        null, null, null, null, null, null, null, null, null, null, null
                    },
                    new object[]
                    {
                        TDengineConstant.ConvertTimestampToDateTime(timeStampes[2], precision), v1, v2, v3, v4, v5, v6,
                        v7,
                        v8, v9, v10,
                        v11,
                        Encoding.UTF8.GetBytes("中文"),
                        "中文", Encoding.UTF8.GetBytes("中文"),
                        new byte[]
                        {
                            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                        },
                        v16, v17,
                    },
                    new object[]
                    {
                        TDengineConstant.ConvertTimestampToDateTime(timeStampes[3], precision), v1_3, v2_3, v3_3, v4_3,
                        v5_3,
                        v6_3, v7_3, v8_3, v9_3, v10_3,
                        v11_3,
                        Encoding.UTF8.GetBytes("中文"),
                        "中文", Encoding.UTF8.GetBytes("中文"),
                        new byte[]
                        {
                            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                        },
                        v16_3, v17_3,
                    },
                    new object[]
                    {
                        TDengineConstant.ConvertTimestampToDateTime(timeStampes[4], precision), v1_4, v2_4, v3_4, v4_4,
                        v5_4,
                        v6_4, v7_4, v8_4, v9_4, v10_4,
                        v11_4,
                        Encoding.UTF8.GetBytes("中文"),
                        "中文", Encoding.UTF8.GetBytes("中文"),
                        new byte[]
                        {
                            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                        },
                        v16_4, v17_4,
                    },
                };
            }

            sql = $"values" +
                  $"({timeStampes[0]},{v1},{v2},{v3},{v4},{v5},{v6},{v7},{v8},{v9},{v10},{v11},'test_binary','test_nchar','中文','POINT(100 100)')" +
                  $"({timeStampes[1]},null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)" +
                  $"({timeStampes[2]},{v1},{v2},{v3},{v4},{v5},{v6},{v7},{v8},{v9},{v10},{v11},'中文','中文','中文','POINT(100 100)')" +
                  $"({timeStampes[3]},{v1_3},{v2_3},{v3_3},{v4_3},{v5_3},{v6_3},{v7_3},{v8_3},{v9_3},{v10_3},{v11_3},'中文','中文','中文','POINT(100 100)')" +
                  $"({timeStampes[4]},{v1_4},{v2_4},{v3_4},{v4_4},{v5_4},{v6_4},{v7_4},{v8_4},{v9_4},{v10_4},{v11_4},'中文','中文','中文','POINT(100 100)')";
            return new object[][]
            {
                new object[]
                {
                    TDengineConstant.ConvertTimestampToDateTime(timeStampes[0], precision, tz), v1, v2, v3, v4, v5, v6,
                    v7,
                    v8,
                    v9, v10,
                    v11,
                    Encoding.UTF8.GetBytes("test_binary"),
                    "test_nchar", Encoding.UTF8.GetBytes("中文"),
                    new byte[]
                    {
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                    },
                },
                new object[]
                {
                    TDengineConstant.ConvertTimestampToDateTime(timeStampes[1], precision, tz), null, null, null, null,
                    null,
                    null,
                    null, null, null, null, null, null, null, null, null
                },
                new object[]
                {
                    TDengineConstant.ConvertTimestampToDateTime(timeStampes[2], precision, tz), v1, v2, v3, v4, v5, v6,
                    v7,
                    v8,
                    v9, v10,
                    v11,
                    Encoding.UTF8.GetBytes("中文"),
                    "中文", Encoding.UTF8.GetBytes("中文"),
                    new byte[]
                    {
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                    },
                },
                new object[]
                {
                    TDengineConstant.ConvertTimestampToDateTime(timeStampes[3], precision, tz), v1_3, v2_3, v3_3, v4_3,
                    v5_3,
                    v6_3, v7_3, v8_3, v9_3, v10_3,
                    v11_3,
                    Encoding.UTF8.GetBytes("中文"),
                    "中文", Encoding.UTF8.GetBytes("中文"),
                    new byte[]
                    {
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                    },
                },
                new object[]
                {
                    TDengineConstant.ConvertTimestampToDateTime(timeStampes[4], precision, tz), v1_4, v2_4, v3_4, v4_4,
                    v5_4,
                    v6_4, v7_4, v8_4, v9_4, v10_4,
                    v11_4,
                    Encoding.UTF8.GetBytes("中文"),
                    "中文", Encoding.UTF8.GetBytes("中文"),
                    new byte[]
                    {
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                    },
                },
            };
        }

        private static string GenerateCreateTableSql(string tableName, bool withDecimal)
        {
            var commonColumns = new StringBuilder()
                .Append($"create table if not exists {tableName} (ts timestamp,")
                .Append("c1 bool,")
                .Append("c2 tinyint,")
                .Append("c3 smallint,")
                .Append("c4 int,")
                .Append("c5 bigint,")
                .Append("c6 tinyint unsigned,")
                .Append("c7 smallint unsigned,")
                .Append("c8 int unsigned,")
                .Append("c9 bigint unsigned,")
                .Append("c10 float,")
                .Append("c11 double,")
                .Append("c12 binary(20),")
                .Append("c13 nchar(20),")
                .Append("c14 varbinary(20),")
                .Append("c15 geometry(100)");

            if (withDecimal)
            {
                commonColumns
                    .Append(",c16 decimal(20,4),")
                    .Append("c17 decimal(8,4)");
            }

            commonColumns.Append(") tags(t json)");

            return commonColumns.ToString();
        }

        private static Array[] TransposeToTypedArrays(object[][] data)
        {
            if (data == null || data.Length == 0)
                throw new ArgumentException("Data cannot be null or empty", nameof(data));

            int rowCount = data.Length;

            return new Array[]
            {
                CreateColumnArray<DateTime>(data, 0, o => (DateTime)o),
                CreateNullableColumnArray<bool>(data, 1, o => (bool)o),
                CreateNullableColumnArray<sbyte>(data, 2, o => (sbyte)o),
                CreateNullableColumnArray<short>(data, 3, o => (short)o),
                CreateNullableColumnArray<int>(data, 4, o => (int)o),
                CreateNullableColumnArray<long>(data, 5, o => (long)o),
                CreateNullableColumnArray<byte>(data, 6, o => (byte)o),
                CreateNullableColumnArray<ushort>(data, 7, o => (ushort)o),
                CreateNullableColumnArray<uint>(data, 8, o => (uint)o),
                CreateNullableColumnArray<ulong>(data, 9, o => (ulong)o),
                CreateNullableColumnArray<float>(data, 10, o => (float)o),
                CreateNullableColumnArray<double>(data, 11, o => (double)o),
                CreateColumnArray<byte[]>(data, 12, o => (byte[])o),
                CreateColumnArray<string>(data, 13, o => (string)o),
                CreateColumnArray<byte[]>(data, 14, o => (byte[])o),
                CreateColumnArray<byte[]>(data, 15, o => (byte[])o)
            };
        }

        private static T[] CreateColumnArray<T>(object[][] data, int columnIndex, Func<object, T> converter)
        {
            T[] array = new T[data.Length];
            for (int i = 0; i < data.Length; i++)
            {
                array[i] = converter(data[i][columnIndex]);
            }

            return array;
        }

        private static T?[] CreateNullableColumnArray<T>(object[][] data, int columnIndex, Func<object, T> converter)
            where T : struct
        {
            T?[] array = new T?[data.Length];
            for (int i = 0; i < data.Length; i++)
            {
                array[i] = data[i][columnIndex] != null ? converter(data[i][columnIndex]) : (T?)null;
            }

            return array;
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
                default:
                    throw new ArgumentOutOfRangeException(nameof(precision), precision, null);
            }
        }

        private static bool IsCloudTest(ConnectionStringBuilder builder)
        {
            return !string.IsNullOrEmpty(builder.Token);
        }

        private void QueryTest(string connectString, string db, TDenginePrecision precision)
        {
            var withDecimal = true;
            var data = this.GenerateValue(precision, withDecimal, out var insertSql);
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
                    var createTableSql = GenerateCreateTableSql(superTableName, withDecimal);
                    client.Exec(createTableSql);
                    string insertQuery =
                        $"insert into {subTableName} using {superTableName} tags('{{\"a\":\"b\"}}') {insertSql}";
                    client.Exec(insertQuery);
                    string query = $"select * from {superTableName} order by ts asc";
                    using (var rows = client.Query(query))
                    {
                        this.AssertColumn(rows, withDecimal);
                        this.AssertValue(rows, data, precision);
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
            var withDecimal = true;
            var data = this.GenerateValue(precision, withDecimal, out var insertSql);
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
                    string createTableSql = GenerateCreateTableSql(superTableName, withDecimal);
                    client.Exec(createTableSql, ReqId.GetReqId());
                    string insertQuery =
                        $"insert into {subTableName} using {superTableName} tags('{{\"a\":\"b\"}}') {insertSql}";
                    client.Exec(insertQuery, ReqId.GetReqId());
                    string query = $"select * from {superTableName} order by ts asc";
                    using (var rows = client.Query(query, ReqId.GetReqId()))
                    {
                        this.AssertColumn(rows, withDecimal);
                        this.AssertValue(rows, data, precision);
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
            var withDecimal = false;
            var data = this.GenerateValue(precision, withDecimal, out _);
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
                    var createTableSql = GenerateCreateTableSql(superTableName, withDecimal);
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
                    var rowCount = data.Length;
                    for (int i = 0; i < rowCount; i++)
                    {
                        stmt.BindRow(data[i]);
                    }

                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)rowCount, affected);
                    stmt.Prepare($"select * from {superTableName} where ts >= ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[] { data[0][0] });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        this.AssertColumn(rows, withDecimal);
                        this.AssertValue(rows, data, precision);
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

        private void StmtTestWrongType(string connectString, string db, TDenginePrecision precision)
        {
            var builder = new ConnectionStringBuilder(connectString);
            using (var client = DbDriver.Open(builder))
            {
                var now = DateTime.Now;
                try
                {
                    client.Exec($"drop database if exists {db}");
                    client.Exec($"create database {db} precision '{PrecisionString(precision)}'");

                    client.Exec($"use {db}");
                    // timestamp
                    client.Exec($"create table if not exists test_ts (ts timestamp, c1 timestamp)");
                    // bool
                    client.Exec($"create table if not exists test_bool (ts timestamp, c1 bool)");
                    // tinyint
                    client.Exec($"create table if not exists test_i8 (ts timestamp, ci tinyint)");
                    // smallint
                    client.Exec($"create table if not exists test_i16 (ts timestamp, ci smallint)");
                    // int
                    client.Exec($"create table if not exists test_i32 (ts timestamp, ci int)");
                    // bigint
                    client.Exec($"create table if not exists test_i64 (ts timestamp, ci bigint)");
                    // tinyint unsigned
                    client.Exec($"create table if not exists test_u8 (ts timestamp, ci tinyint unsigned)");
                    // smallint unsigned
                    client.Exec($"create table if not exists test_u16 (ts timestamp, ci smallint unsigned)");
                    // int unsigned
                    client.Exec($"create table if not exists test_u32 (ts timestamp, ci int unsigned)");
                    // bigint unsigned
                    client.Exec($"create table if not exists test_u64 (ts timestamp, ci bigint unsigned)");
                    // float
                    client.Exec($"create table if not exists test_f32 (ts timestamp, c1 float)");
                    // double
                    client.Exec($"create table if not exists test_f64 (ts timestamp, c1 double)");
                    // binary
                    client.Exec($"create table if not exists test_binary (ts timestamp, c1 binary(100))");
                    // nchar
                    client.Exec($"create table if not exists test_nchar (ts timestamp, c1 nchar(100))");
                    // varbinary
                    client.Exec($"create table if not exists test_varbinary (ts timestamp, c1 varbinary(100))");
                    // geometry
                    client.Exec($"create table if not exists test_geometry (ts timestamp, c1 geometry(100))");
                    // json
                    client.Exec($"create table if not exists test_json_stb (ts timestamp, c1 int) tags(t json)");
                    using (var stmt = client.StmtInit())
                    {
                        // json
                        var sql = $"insert into ? using test_json_stb tags(?) values(?,?)";
                        _output.WriteLine($"{sql}");
                        stmt.Prepare(sql);
                        stmt.SetTableName("test_json");
                        stmt.SetTags(new object[] { "{\"a\":\"b\"}" });
                        stmt.BindRow(new object[] { DateTime.Now, 1 });
                        stmt.AddBatch();
                        stmt.Exec();
                        var affected = stmt.Affected();
                        Assert.Equal(1, affected);
                        using (var rows = client.Query("select count(*) from test_json_stb"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(1, rows.GetInt32(0));
                        }
                        stmt.SetTableName("test_json_null");
                        stmt.SetTags(new object[] { null });
                        stmt.BindRow(new object[] { DateTime.Now, 1 });
                        stmt.AddBatch();
                        stmt.Exec();
                        affected = stmt.Affected();
                        Assert.Equal(1, affected);
                        using (var rows = client.Query("select count(*) from test_json_stb"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(2, rows.GetInt32(0));
                        }
                        // ts
                        sql = $"insert into test_ts values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP);
                        using (var rows = client.Query("select count(*) from test_ts"))
                        {
                            Assert.True(rows.Read());
                            // null + DateTime * 3 + long * 3 + DateTimeOffset * 3
                            Assert.Equal(10, rows.GetInt32(0));
                        }
                        // bool
                        sql = $"insert into test_bool values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_BOOL);
                        using (var rows = client.Query("select count(*) from test_bool"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // tinyint
                        sql = $"insert into test_i8 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_TINYINT);
                        using (var rows = client.Query("select count(*) from test_i8"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // smallint
                        sql = $"insert into test_i16 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_SMALLINT);
                        using (var rows = client.Query("select count(*) from test_i16"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // int
                        sql = $"insert into test_i32 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_INT);
                        using (var rows = client.Query("select count(*) from test_i32"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // bigint
                        sql = $"insert into test_i64 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_BIGINT);
                        using (var rows = client.Query("select count(*) from test_i64"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // tinyint unsigned
                        sql = $"insert into test_u8 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_UTINYINT);
                        using (var rows = client.Query("select count(*) from test_u8"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // smallint unsigned
                        sql = $"insert into test_u16 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_USMALLINT);
                        using (var rows = client.Query("select count(*) from test_u16"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // int unsigned
                        sql = $"insert into test_u32 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_UINT);
                        using (var rows = client.Query("select count(*) from test_u32"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // bigint unsigned
                        sql = $"insert into test_u64 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_UBIGINT);
                        using (var rows = client.Query("select count(*) from test_u64"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // float
                        sql = $"insert into test_f32 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_FLOAT);
                        using (var rows = client.Query("select count(*) from test_f32"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // double
                        sql = $"insert into test_f64 values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_DOUBLE);
                        using (var rows = client.Query("select count(*) from test_f64"))
                        {
                            Assert.True(rows.Read());
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // binary
                        sql = $"insert into test_binary values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_BINARY);
                        using (var rows = client.Query("select count(*) from test_binary"))
                        {
                            Assert.True(rows.Read());
                            // null + byte[] * 3 + string * 3
                            Assert.Equal(7, rows.GetInt32(0));
                        }
                        // nchar
                        sql = $"insert into test_nchar values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_NCHAR);
                        using (var rows = client.Query("select count(*) from test_nchar"))
                        {
                            Assert.True(rows.Read());
                            // null + string * 3
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                        // varbinary
                        sql = $"insert into test_varbinary values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
                        using (var rows = client.Query("select count(*) from test_varbinary"))
                        {
                            Assert.True(rows.Read());
                            // null + byte[] * 3 + string * 3
                            Assert.Equal(7, rows.GetInt32(0));
                        }
                        // geometry
                        sql = $"insert into test_geometry values(?,?)";
                        _output.WriteLine($"{sql}");
                        doStmtTest(client, stmt, sql, TDengineDataType.TSDB_DATA_TYPE_GEOMETRY);
                        using (var rows = client.Query("select count(*) from test_geometry"))
                        {
                            Assert.True(rows.Read());
                            // null + byte[] * 3
                            Assert.Equal(4, rows.GetInt32(0));
                        }
                    }
                }
                catch (Exception e)
                {
                    _output.WriteLine(e.ToString());
                    throw;
                }
                finally
                {
                    client.Exec($"drop database if exists {db}");
                }
            }
        }

        private void doStmtTest(ITDengineClient client, IStmt stmt, string sql, TDengineDataType dataType)
        {
            var now = DateTime.UtcNow;
            stmt.Prepare(sql);
            var isInsert = stmt.IsInsert();
            Assert.True(isInsert);
            var colFields = stmt.GetColFields();
            now = now.AddSeconds(1);
            var rowData = new List<object>
            {
                now,
                null
            };
            stmt.BindRow(rowData.ToArray());
            stmt.AddBatch();
            stmt.Exec();
            Assert.Equal((long)1, stmt.Affected());
            
            // DateTime
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                now
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            var colData = new Array[2]
            {
                new DateTime[] { now },
                new DateTime[] { now },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new DateTime?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // DateTimeOffset
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                DateTimeOffset.UtcNow,
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new DateTimeOffset[] { DateTimeOffset.UtcNow },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new DateTimeOffset?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // bool 
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                true
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BOOL)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new bool[] { false },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BOOL)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new bool?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BOOL)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            // sbyte
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (sbyte)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TINYINT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new sbyte[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TINYINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new sbyte?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TINYINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // short
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (short)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_SMALLINT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new short[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_SMALLINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new short?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_SMALLINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // int
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (int)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_INT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new int[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_INT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new int?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_INT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // long
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (long)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_BIGINT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new long[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_BIGINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new long?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_BIGINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // float
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (float)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_FLOAT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new float[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_FLOAT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new float?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_FLOAT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // double
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (double)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_DOUBLE)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new double[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_DOUBLE)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new double?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_DOUBLE)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // byte
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (byte)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UTINYINT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new byte[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UTINYINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new byte?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UTINYINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // ushort
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (ushort)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_USMALLINT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new ushort[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_USMALLINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new ushort?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_USMALLINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }


            // uint
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (uint)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UINT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new uint[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new uint?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // ulong
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                (ulong)2
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UBIGINT)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new ulong[] { 2 },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UBIGINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new ulong?[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_UBIGINT)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // string
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                "abc",
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_NCHAR ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_VARBINARY)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new string[] { "abc" },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_NCHAR ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_VARBINARY)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new string[] { null },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_NCHAR ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_VARBINARY)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }

            // byte[]
            now = now.AddSeconds(1);
            rowData = new List<object>
            {
                now,
                new byte[]
                {
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_GEOMETRY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_VARBINARY)
            {
                stmt.BindRow(rowData.ToArray());
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindRow(rowData.ToArray()));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new byte[][]
                {
                    new byte[]
                    {
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                    },
                },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_GEOMETRY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_VARBINARY)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
            now = now.AddSeconds(1);
            colData = new Array[2]
            {
                new DateTime[] { now },
                new byte[][]
                {
                    null,
                },
            };
            if (dataType == TDengineDataType.TSDB_DATA_TYPE_BINARY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_GEOMETRY ||
                dataType == TDengineDataType.TSDB_DATA_TYPE_VARBINARY)
            {
                stmt.BindColumn(colFields, colData);
                stmt.AddBatch();
                stmt.Exec();
                Assert.Equal((long)1, stmt.Affected());
            }
            else
            {
                Assert.Throws<ArgumentException>(() => stmt.BindColumn(colFields, colData));
            }
        }


        private void StmtWithReqIDTest(string connectString, string db, TDenginePrecision precision)
        {
            var withDecimal = false;
            var data = this.GenerateValue(precision, withDecimal, out _);
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
                    var createTableSql = GenerateCreateTableSql(superTableName, withDecimal);
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
                    var rowCount = data.Length;
                    for (int i = 0; i < rowCount; i++)
                    {
                        stmt.BindRow(data[i]);
                    }

                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)rowCount, affected);
                    stmt.Prepare($"select * from {superTableName} where ts >= ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[] { data[0][0] });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        this.AssertColumn(rows, withDecimal);
                        this.AssertValue(rows, data, precision);
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
            var withDecimal = false;
            var data = this.GenerateValue(precision, withDecimal, out _);
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
                    var createTableSql = GenerateCreateTableSql(superTableName, withDecimal);
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
                    Assert.Equal((long)data.Length, affected);
                    stmt.Prepare($"select * from {superTableName} where ts >= ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[] { data[0][0] });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var result = stmt.Result())
                    {
                        this.AssertColumn(result, withDecimal);
                        this.AssertValue(result, data, precision);
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
            var now = TDengineConstant.ConvertTimestampToDateTime(ts, TDenginePrecision.TSDB_TIME_PRECISION_MILLI);
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

        private void AssertColumn(IRows result, bool withDecimal)
        {
            Assert.Equal(1, result.GetOrdinal("c1"));
            var fieldCount = result.FieldCount;
            if (withDecimal)
            {
                Assert.Equal(19, fieldCount);
            }
            else
            {
                Assert.Equal(17, fieldCount);
            }

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
            if (withDecimal)
            {
                Assert.Equal("c16", result.GetName(16));
                Assert.Equal("c17", result.GetName(17));
                Assert.Equal("t", result.GetName(18));
            }
            else
            {
                Assert.Equal("t", result.GetName(16));
            }

            Assert.Equal(-1, result.AffectRows);
        }

        private void AssertValue(IRows rows, object[][] data, TDenginePrecision precision)
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
                    CheckValue(val, expectVal);
                }

                for (int j = 0; j < data[i].Length; j++)
                {
                    switch (data[i][j])
                    {
                        case DateTime dtValue:
                            CheckValue(rows.GetDateTime(j), data[i][j]);
                            CheckValue(TDengineConstant.ConvertDateTimeOffsetToTimestamp(rows.GetDateTimeOffset(j),
                                    precision),
                                TDengineConstant.ConvertDateTimeToTimestamp(dtValue,
                                    precision));
                            CheckValue(rows.GetInt64(j), TDengineConstant.ConvertDateTimeToTimestamp(dtValue,
                                precision));
                            break;
                        case bool boolValue:
                            Assert.Equal(boolValue, rows.GetBoolean(j));
                            break;
                        case short shortValue:
                            Assert.Equal(shortValue, rows.GetInt16(j));
                            break;
                        case int intValue:
                            Assert.Equal(intValue, rows.GetInt32(j));
                            break;
                        case long longValue:
                            Assert.Equal(longValue, rows.GetInt64(j));
                            break;
                        case byte byteValue:
                            Assert.Equal(byteValue, rows.GetByte(j));
                            break;
                        case float floatValue:
                            CheckValue(rows.GetFloat(j), floatValue);
                            break;
                        case double doubleValue:
                            CheckValue(rows.GetDouble(j), doubleValue);
                            break;
                        case decimal decimalValue:
                            Assert.Equal(decimalValue, rows.GetDecimal(j));
                            break;
                        case string stringValue:
                            Assert.Equal(stringValue, rows.GetString(j));
                            break;
                    }
                }

                Assert.Equal(Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), rows.GetValue(data[i].Length));
            }
        }

        private static void CheckValue(object val, object expectVal)
        {
#if NETFRAMEWORK
            const float floatTolerance = 0.00001f;
            const double doubleTolerance = 0.0000000000001;
            if (val is float floatVal)
            {
                Assert.IsType<float>(expectVal);
                Assert.True(Math.Abs((float)expectVal - floatVal) < floatTolerance);
            }
            else if (val is double doubleVal)
            {
                Assert.IsType<double>(expectVal);
                Assert.True(Math.Abs((double)expectVal - doubleVal) < doubleTolerance);
            }
            else
            {
                Assert.Equal(expectVal, val);
            }
#else
            Assert.Equal(expectVal, val);
#endif
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
                    tsv[i] = TDengineConstant.ConvertTimestampToDateTime(ts[i], precision);
                }

                var valuesStr = "";
                for (int i = 0; i < count; i++)
                {
                    valuesStr += $"({ts[i]}, {i}, {i}, '中文')";
                }

                client.Exec($"insert into {tableName} values {valuesStr}");
                var tasks = new List<Task>();
                for (var i = 0; i < count; i++)
                {
                    int localI = i;
                    string query = $"select * from {tableName} where ts = " + ts[localI];
                    tasks.Add(Task.Run(() =>
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

                Task.WaitAll(tasks.ToArray());
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

        private void QueryWithConnectionTimezoneTest(string connectString, string connectionTimezone, string db,
            TDenginePrecision precision)
        {
            if (Environment.Version.Major < 6)
            {
                _output.WriteLine(
                    $"Dotnet Version is {Environment.Version}. Skipping QueryWithConnectionTimezoneTest.");
                return;
            }

            var tz = TimeZoneInfo.FindSystemTimeZoneById(connectionTimezone);
            var builder = new ConnectionStringBuilder(connectString)
            {
                ConnectionTimezone = tz
            };
            var inCloud = IsCloudTest(builder);
            var utcBuilder = new ConnectionStringBuilder(connectString)
            {
                ConnectionTimezone = TimeZoneInfo.Utc
            };
            ITDengineClient utcClient = null;
            ITDengineClient client = null;
            try
            {
                utcClient = DbDriver.Open(utcBuilder);
                client = DbDriver.Open(builder);
            }
            catch (TDengineError e)
            {
                if (e.Code != 0x237) throw;
                _output.WriteLine(
                    $"TDengineError: {e.Code} - {e.Message}. Skipping QueryWithConnectionTimezoneTest.");
                return;
            }

            try
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
                    utcClient.Exec($"use {db}", ReqId.GetReqId());
                    var createTableSql =
                        $"create table if not exists {superTableName} (ts timestamp,v int) tags (tg int)";
                    client.Exec(createTableSql, ReqId.GetReqId());

                    var ts = TDengineConstant.ConvertDateTimeToTimestamp(now, precision);
                    var targetTime = TDengineConstant.ConvertTimestampToDateTime(ts, precision, tz);
                    var utcTime = TDengineConstant.ConvertTimestampToDateTime(ts, precision, TimeZoneInfo.Utc);
                    string timeFormat;
                    switch (precision)
                    {
                        case TDenginePrecision.TSDB_TIME_PRECISION_MILLI:
                            timeFormat = "yyyy-MM-dd HH:mm:ss.fff";
                            break;
                        case TDenginePrecision.TSDB_TIME_PRECISION_MICRO:
                            timeFormat = "yyyy-MM-dd HH:mm:ss.ffffff";
                            break;
                        case TDenginePrecision.TSDB_TIME_PRECISION_NANO:
                            timeFormat = "yyyy-MM-dd HH:mm:ss.fffffff";
                            break;
                        default:
                            throw new NotSupportedException($"unknown precision {precision}");
                    }

                    var insertTime = utcTime.ToString(timeFormat);
                    string insertQuery =
                        $"insert into {subTableName} using {superTableName} tags('1') values('{insertTime}',1)";
                    _output.WriteLine("SQL: " + insertQuery);
                    utcClient.Exec(insertQuery, ReqId.GetReqId());
                    string query = $"select * from {superTableName} order by ts asc";
                    using (var rows = client.Query(query, ReqId.GetReqId()))
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                        this._output.WriteLine($"{((DateTime)rows.GetValue(0)).ToString(timeFormat)}");
                        Assert.Equal(((DateTime)rows.GetValue(0)).ToString(timeFormat),
                            targetTime.ToString(timeFormat));
                        Assert.Equal(ts, rows.GetInt64(0));
                        Assert.Equal(targetTime, rows.GetDateTime(0));
                        Assert.Equal(ts,
                            TDengineConstant.ConvertDateTimeOffsetToTimestamp(rows.GetDateTimeOffset(0), precision));
                        Assert.Equal((int)(1), rows.GetValue(2));
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
            finally
            {
                utcClient?.Dispose();

                client?.Dispose();
            }
        }

        private void StmtBindTimestampTest(string connectString, string db, TDenginePrecision precision)
        {
            var builder = new ConnectionStringBuilder(connectString);
            var inCloud = IsCloudTest(builder);
            using (var client = DbDriver.Open(builder))
            {
                var now = DateTime.Now;
                var ts = TDengineConstant.ConvertDateTimeToTimestamp(now, precision);
                var nextSecond = now.AddSeconds(1);
                var nextSecondTs = TDengineConstant.ConvertDateTimeToTimestamp(nextSecond, precision);
                var next2Second = now.AddSeconds(2);
                var next2SecondTs = TDengineConstant.ConvertDateTimeToTimestamp(next2Second, precision);
                var next3Second = now.AddSeconds(3);
                var next3SecondTs = TDengineConstant.ConvertDateTimeToTimestamp(next3Second, precision);
                var superTableName = $"timestamp_stb_{now.Ticks}";
                var subTableName = $"timestamp_ctb_{now.Ticks}";
                try
                {
                    if (!inCloud)
                    {
                        client.Exec($"drop database if exists {db}", ReqId.GetReqId());
                        client.Exec($"create database {db} precision '{PrecisionString(precision)}'", ReqId.GetReqId());
                    }

                    client.Exec($"use {db}", ReqId.GetReqId());
                    var createTableSql =
                        $"create table if not exists {superTableName} (ts timestamp, v int) tags (t_tag timestamp)";
                    client.Exec(createTableSql, ReqId.GetReqId());
                    var stmt = client.StmtInit(ReqId.GetReqId());
                    // bind row
                    stmt.Prepare($"insert into ? using {superTableName} tags(?) values(?,?)");
                    var isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName(subTableName);
                    stmt.SetTags(new object[]
                        { TDengineConstant.ConvertTimestampToDateTimeOffset(ts, precision, TimeZoneInfo.Utc) });
                    stmt.BindRow(new object[]
                        { TDengineConstant.ConvertTimestampToDateTimeOffset(ts, precision, TimeZoneInfo.Utc), 1 });
                    stmt.AddBatch();
                    stmt.Exec();
                    var affected = stmt.Affected();
                    Assert.Equal((long)1, affected);
                    stmt.Prepare($"select * from {superTableName} where ts = ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[]
                        { TDengineConstant.ConvertTimestampToDateTimeOffset(ts, precision, TimeZoneInfo.Utc) });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                        Assert.Equal("ts", rows.GetName(0));
                        Assert.Equal("v", rows.GetName(1));
                        Assert.Equal("t_tag", rows.GetName(2));
                        CheckValue(TDengineConstant.ConvertDateTimeToTimestamp(rows.GetDateTime(0), precision), ts);
                        CheckValue(TDengineConstant.ConvertDateTimeOffsetToTimestamp(rows.GetDateTimeOffset(0),
                            precision), ts);
                        CheckValue(rows.GetInt64(0), ts);
                    }


                    // bind column
                    stmt.Prepare($"insert into ? using {superTableName} tags(?) values(?,?)");
                    isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName(subTableName);
                    stmt.SetTags(new object[]
                        { TDengineConstant.ConvertTimestampToDateTimeOffset(ts, precision, TimeZoneInfo.Utc) });
                    stmt.BindColumn(stmt.GetColFields(),
                        new DateTimeOffset[]
                        {
                            TDengineConstant.ConvertTimestampToDateTimeOffset(nextSecondTs, precision, TimeZoneInfo.Utc)
                        },
                        new int[] { 1 });
                    stmt.AddBatch();
                    stmt.Exec();
                    affected = stmt.Affected();
                    Assert.Equal((long)1, affected);
                    stmt.Prepare($"select * from {superTableName} where ts = ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[]
                    {
                        TDengineConstant.ConvertTimestampToDateTimeOffset(nextSecondTs, precision, TimeZoneInfo.Utc)
                    });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                        Assert.Equal("ts", rows.GetName(0));
                        Assert.Equal("v", rows.GetName(1));
                        Assert.Equal("t_tag", rows.GetName(2));
                        CheckValue(TDengineConstant.ConvertDateTimeToTimestamp(rows.GetDateTime(0), precision),
                            nextSecondTs);
                        CheckValue(TDengineConstant.ConvertDateTimeOffsetToTimestamp(rows.GetDateTimeOffset(0),
                            precision), nextSecondTs);
                        CheckValue(rows.GetInt64(0), nextSecondTs);
                    }

                    // bind column with DateTimeOffset?[]
                    stmt.Prepare($"insert into ? using {superTableName} tags(?) values(?,?)");
                    isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName(subTableName);
                    stmt.SetTags(new object[]
                        { TDengineConstant.ConvertTimestampToDateTimeOffset(ts, precision, TimeZoneInfo.Utc) });
                    stmt.BindColumn(stmt.GetColFields(),
                        new DateTimeOffset?[]
                        {
                            TDengineConstant.ConvertTimestampToDateTimeOffset(next2SecondTs, precision,
                                TimeZoneInfo.Utc)
                        },
                        new int?[] { 1 });
                    stmt.AddBatch();
                    stmt.Exec();
                    affected = stmt.Affected();
                    Assert.Equal((long)1, affected);
                    stmt.Prepare($"select * from {superTableName} where ts = ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[]
                    {
                        TDengineConstant.ConvertTimestampToDateTimeOffset(next2SecondTs, precision, TimeZoneInfo.Utc)
                    });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                        Assert.Equal("ts", rows.GetName(0));
                        Assert.Equal("v", rows.GetName(1));
                        Assert.Equal("t_tag", rows.GetName(2));
                        CheckValue(TDengineConstant.ConvertDateTimeToTimestamp(rows.GetDateTime(0), precision),
                            next2SecondTs);
                        CheckValue(TDengineConstant.ConvertDateTimeOffsetToTimestamp(rows.GetDateTimeOffset(0),
                            precision), next2SecondTs);
                        CheckValue(rows.GetInt64(0), next2SecondTs);
                    }


                    // bind row with long
                    stmt.Prepare($"insert into ? using {superTableName} tags(?) values(?,?)");
                    isInsert = stmt.IsInsert();
                    Assert.True(isInsert);
                    stmt.SetTableName(subTableName);
                    stmt.SetTags(new object[]
                        { TDengineConstant.ConvertTimestampToDateTimeOffset(ts, precision, TimeZoneInfo.Utc) });
                    stmt.BindRow(new object[] { next3SecondTs, 1 });
                    stmt.AddBatch();
                    stmt.Exec();
                    affected = stmt.Affected();
                    Assert.Equal((long)1, affected);
                    stmt.Prepare($"select * from {superTableName} where ts = ? order by ts asc");
                    isInsert = stmt.IsInsert();
                    Assert.False(isInsert);
                    stmt.BindRow(new object[]
                    {
                        next3SecondTs
                    });
                    stmt.AddBatch();
                    stmt.Exec();
                    using (var rows = stmt.Result())
                    {
                        var haveNext = rows.Read();
                        Assert.True(haveNext);
                        Assert.Equal("ts", rows.GetName(0));
                        Assert.Equal("v", rows.GetName(1));
                        Assert.Equal("t_tag", rows.GetName(2));
                        CheckValue(TDengineConstant.ConvertDateTimeToTimestamp(rows.GetDateTime(0), precision),
                            next3SecondTs);
                        CheckValue(TDengineConstant.ConvertDateTimeOffsetToTimestamp(rows.GetDateTimeOffset(0),
                            precision), next3SecondTs);
                        CheckValue(rows.GetInt64(0), next3SecondTs);
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
    }
}