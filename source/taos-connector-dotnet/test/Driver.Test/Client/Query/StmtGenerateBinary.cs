using System;
using System.Text;
using TDengine.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Client.Query
{
    struct TestCase
    {
        public string TestName;
        public bool IsInsert;
        public TaosFieldAll[] TaosField;
        public int FieldCount;
        public BindData[] BindData;
        public byte[] ExpectData;
        public int ExpectRows;
    }

    struct BindData
    {
        public string TableName;
        public object[] Tags;
        public object[][] Rows;
    }

    public class StmtGenerateBinary
    {
        private ITestOutputHelper _output;

        public StmtGenerateBinary(ITestOutputHelper output)
        {
            _output = output;
        }


        [Fact]
        public void StmtGenerateBinaryTest()
        {
            TestCase[] testCases = new TestCase[]
            {
                new TestCase
                {
                    TestName = "TestAllData",
                    IsInsert = true,
                    TaosField = new TaosFieldAll[]
                    {
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TBNAME,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP,
                            precision = (byte)TDenginePrecision.TSDB_TIME_PRECISION_MILLI,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BOOL,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_SMALLINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_INT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_DOUBLE,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UTINYINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_USMALLINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UBIGINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_NCHAR,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_GEOMETRY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },

                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP,
                            precision = (byte)TDenginePrecision.TSDB_TIME_PRECISION_MILLI,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BOOL,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_TINYINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_SMALLINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_INT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_FLOAT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_DOUBLE,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UTINYINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_USMALLINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_UBIGINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_NCHAR,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_GEOMETRY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_VARBINARY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                    },
                    FieldCount = 0,
                    BindData = new BindData[]
                    {
                        new BindData
                        {
                            TableName = "test1",
                            Tags = new object[]
                            {
                                (long)1726803356466,
                                true,
                                (sbyte)1,
                                (short)2,
                                (int)3,
                                (long)4,
                                (float)5.5,
                                (double)6.6,
                                (byte)7,
                                (ushort)8,
                                (uint)9,
                                (ulong)10,
                                Encoding.UTF8.GetBytes("binary"),
                                "nchar",
                                new byte[]
                                {
                                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00,
                                    0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                                },
                                Encoding.UTF8.GetBytes("varbinary"),
                            },
                            Rows = new object[][]
                            {
                                new object[]
                                {
                                    (long)1726803356466,
                                    true,
                                    (sbyte)11,
                                    (short)11,
                                    (int)11,
                                    (long)11,
                                    (float)11.2,
                                    (double)11.2,
                                    (byte)11,
                                    (ushort)11,
                                    (uint)11,
                                    (ulong)11,
                                    Encoding.UTF8.GetBytes("binary1"),
                                    "nchar1",
                                    new byte[]
                                    {
                                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
                                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                                    },
                                    Encoding.UTF8.GetBytes("varbinary1"),
                                },
                                new object[]
                                {
                                    (long)1726803357466,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                },
                                new object[]
                                {
                                    (long)1726803358466,
                                    false,
                                    (sbyte)12,
                                    (short)12,
                                    (int)12,
                                    (long)12,
                                    (float)12.2,
                                    (double)12.2,
                                    (byte)12,
                                    (ushort)12,
                                    (uint)12,
                                    (ulong)12,
                                    Encoding.UTF8.GetBytes("binary2"),
                                    "nchar2",
                                    new byte[]
                                    {
                                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
                                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40
                                    },
                                    Encoding.UTF8.GetBytes("varbinary2"),
                                }
                            }
                        }
                    },
                    ExpectData = new byte[]
                    {
                        // TotalLength
                        0x19, 0x04, 0x00, 0x00,
                        // tableCount
                        0x01, 0x00, 0x00, 0x00,
                        // TagCount
                        0x10, 0x00, 0x00, 0x00,
                        // ColCount
                        0x10, 0x00, 0x00, 0x00,
                        // TableNamesOffset
                        0x1c, 0x00, 0x00, 0x00,
                        // TagsOffset
                        0x24, 0x00, 0x00, 0x00,
                        // ColsOffset
                        0xb4, 0x01, 0x00, 0x00,

                        // TableNameLength
                        0x06, 0x00,
                        // TableNameBuffer
                        0x74, 0x65, 0x73, 0x74, 0x31, 0x00,

                        // TagsDataLength
                        0x8c, 0x01, 0x00, 0x00,

                        // TagsBuffer

                        // tag1 timestamp
                        // TotalLength
                        0x1a, 0x00, 0x00, 0x00,
                        // type
                        0x09, 0x00, 0x00, 0x00,
                        // num
                        0x01, 0x00, 0x00, 0x00,
                        // isnull
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x08, 0x00, 0x00, 0x00,
                        // buffer
                        0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

                        // tag2 bool
                        0x13, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x01,

                        // tag3 tinyint
                        0x13, 0x00, 0x00, 0x00,
                        0x02, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x01,

                        // tag4 smallint
                        0x14, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x02, 0x00, 0x00, 0x00,
                        0x02, 0x00,

                        // tag5 int
                        0x16, 0x00, 0x00, 0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,

                        // tag6 bigint
                        0x1a, 0x00, 0x00, 0x00,
                        0x05, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        // tag7 float
                        0x16, 0x00, 0x00, 0x00,
                        0x06, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0xb0, 0x40,

                        // tag8 double
                        0x1a, 0x00, 0x00, 0x00,
                        0x07, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40,

                        // tag9 utinyint
                        0x13, 0x00, 0x00, 0x00,
                        0x0b, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x07,

                        // tag10 usmallint
                        0x14, 0x00, 0x00, 0x00,
                        0x0c, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x02, 0x00, 0x00, 0x00,
                        0x08, 0x00,

                        // tag11 uint
                        0x16, 0x00, 0x00, 0x00,
                        0x0d, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x09, 0x00, 0x00, 0x00,

                        // tag12 ubigint
                        0x1a, 0x00, 0x00, 0x00,
                        0x0e, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        // tag13 binary
                        0x1c, 0x00, 0x00, 0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        // haveLength
                        0x01,
                        // length
                        0x06, 0x00, 0x00, 0x00,
                        //buffer length
                        0x06, 0x00, 0x00, 0x00,
                        0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

                        // tag14 nchar
                        0x1b, 0x00, 0x00, 0x00,
                        0x0a, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x01,
                        0x05, 0x00, 0x00, 0x00,
                        0x05, 0x00, 0x00, 0x00,
                        0x6e, 0x63, 0x68, 0x61, 0x72,

                        // tag15 geometry
                        0x2b, 0x00, 0x00, 0x00,
                        0x14, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x01,
                        0x15, 0x00, 0x00, 0x00,
                        0x15, 0x00, 0x00, 0x00,
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x59, 0x40,

                        // tag16 varbinary
                        0x1f, 0x00, 0x00, 0x00,
                        0x10, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x01,
                        0x09, 0x00, 0x00, 0x00,
                        0x09, 0x00, 0x00, 0x00,
                        0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

                        // ColDataLength
                        0x61, 0x02, 0x00, 0x00,

                        // ColBuffer
                        // col1 timestamp
                        // TotalLength
                        0x2c, 0x00, 0x00, 0x00,
                        // Type
                        0x09, 0x00, 0x00, 0x00,
                        // Num
                        0x03, 0x00, 0x00, 0x00,
                        // IsNull
                        0x00, 0x00, 0x00,
                        //haveLength
                        0x00,
                        // BufferLength
                        0x18, 0x00, 0x00, 0x00,
                        // Buffer
                        0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,
                        0x1a, 0x2f, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,
                        0x02, 0x33, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

                        // col2 bool
                        0x17, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        // is null, row index 1 is null
                        0x00, 0x01, 0x00,
                        0x00,
                        0x03, 0x00, 0x00, 0x00,

                        // row0
                        0x01,
                        // row1
                        0x00,
                        // row2
                        0x00,

                        // col3 tinyint
                        0x17, 0x00, 0x00, 0x00,
                        0x02, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x03, 0x00, 0x00, 0x00,

                        0x0b,
                        0x00,
                        0x0c,

                        // col4 smallint
                        0x1a, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x06, 0x00, 0x00, 0x00,

                        0x0b, 0x00,
                        0x00, 0x00,
                        0x0c, 0x00,

                        // col5 int
                        0x20, 0x00, 0x00, 0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x0c, 0x00, 0x00, 0x00,

                        0x0b, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                        0x0c, 0x00, 0x00, 0x00,

                        // col6 bigint
                        0x2c, 0x00, 0x00, 0x00,
                        0x05, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x18, 0x00, 0x00, 0x00,

                        0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        // col7 float
                        0x20, 0x00, 0x00, 0x00,
                        0x06, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x0c, 0x00, 0x00, 0x00,
                        0x33, 0x33, 0x33, 0x41,
                        0x00, 0x00, 0x00, 0x00,
                        0x33, 0x33, 0x43, 0x41,

                        // col8 double
                        0x2c, 0x00, 0x00, 0x00,
                        0x07, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x18, 0x00, 0x00, 0x00,

                        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x26, 0x40,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x28, 0x40,

                        // col9 utinyint
                        0x17, 0x00, 0x00, 0x00,
                        0x0b, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x03, 0x00, 0x00, 0x00,

                        0x0b,
                        0x00,
                        0x0c,

                        // col10 usmallint
                        0x1a, 0x00, 0x00, 0x00,
                        0x0c, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x06, 0x00, 0x00, 0x00,

                        0x0b, 0x00,
                        0x00, 0x00,
                        0x0c, 0x00,

                        // col11 uint
                        0x20, 0x00, 0x00, 0x00,
                        0x0d, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x0c, 0x00, 0x00, 0x00,

                        0x0b, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                        0x0c, 0x00, 0x00, 0x00,

                        // col12 ubigint
                        0x2C, 0x00, 0x00, 0x00,
                        0x0e, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x00,
                        0x18, 0x00, 0x00, 0x00,

                        0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        // col13 binary
                        0x2e, 0x00, 0x00, 0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        // have length
                        0x01,
                        // length
                        0x07, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                        0x07, 0x00, 0x00, 0x00,
                        // buffer length
                        0x0e, 0x00, 0x00, 0x00,
                        // buffer
                        0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x31,
                        0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,

                        // col14 nchar
                        0x2c, 0x00, 0x00, 0x00,
                        0x0a, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x01,
                        // length
                        0x06, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                        0x06, 0x00, 0x00, 0x00,
                        // buffer length
                        0x0c, 0x00, 0x00, 0x00,
                        // buffer
                        0x6e, 0x63, 0x68, 0x61, 0x72, 0x31,
                        0x6e, 0x63, 0x68, 0x61, 0x72, 0x32,

                        // col15 geometry
                        0x4a, 0x00, 0x00, 0x00,
                        0x14, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x01,
                        // length
                        0x15, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                        0x15, 0x00, 0x00, 0x00,
                        // buffer length
                        0x2a, 0x00, 0x00, 0x00,
                        // buffer
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x59, 0x40,
                        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x59, 0x40,

                        // col16 varbinary
                        0x34, 0x00, 0x00, 0x00,
                        0x10, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,
                        0x00, 0x01, 0x00,
                        0x01,
                        // length
                        0x0a, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                        0x0a, 0x00, 0x00, 0x00,
                        // buffer length
                        0x14, 0x00, 0x00, 0x00,
                        // buffer
                        0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x31,
                        0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,
                    },
                    ExpectRows = 3,
                },
                new TestCase
                {
                    TestName = "Three Table",
                    IsInsert = true,
                    TaosField = new TaosFieldAll[]
                    {
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BINARY,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TBNAME,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP,
                            precision = (byte)TDenginePrecision.TSDB_TIME_PRECISION_MILLI,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_BIGINT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_COL,
                        },
                        new TaosFieldAll
                        {
                            type = (sbyte)TDengineDataType.TSDB_DATA_TYPE_INT,
                            field_type = (byte)TaosFieldType.TAOS_FIELD_TAG,
                        },
                    },
                    FieldCount = 0,
                    BindData = new BindData[]
                    {
                        new BindData
                        {
                            TableName = "table1",
                            Tags = new object[]
                            {
                                (int)1,
                            },
                            Rows = new object[][]
                            {
                                new object[]
                                {
                                    (long)1726803356466,
                                    (long)1,
                                },
                            }
                        },
                        new BindData
                        {
                            TableName = "table2",
                            Tags = new object[]
                            {
                                (int)2,
                            },
                            Rows = new object[][]
                            {
                                new object[]
                                {
                                    (long)1726803356466,
                                    (long)2,
                                },
                            }
                        },
                        new BindData
                        {
                            TableName = "table3",
                            Tags = new object[]
                            {
                                (int)3,
                            },
                            Rows = new object[][]
                            {
                                new object[]
                                {
                                    (long)1726803356466,
                                    (long)3,
                                },
                            }
                        }
                    },
                    ExpectData = new byte[]
                    {
                        // TotalLength
                        0x2d, 0x01, 0x00, 0x00,
                        // tableCount
                        0x03, 0x00, 0x00, 0x00,
                        // TagCount
                        0x01, 0x00, 0x00, 0x00,
                        // ColCount
                        0x02, 0x00, 0x00, 0x00,
                        // TableNamesOffset
                        0x1c, 0x00, 0x00, 0x00,
                        // TagsOffset
                        0x37, 0x00, 0x00, 0x00,
                        // ColsOffset
                        0x85, 0x00, 0x00, 0x00,
                        // TableNameLength
                        0x07, 0x00,
                        0x07, 0x00,
                        0x07, 0x00,
                        // TableNameBuffer
                        0x74, 0x61, 0x62, 0x6c, 0x65, 0x31, 0x00,
                        0x74, 0x61, 0x62, 0x6c, 0x65, 0x32, 0x00,
                        0x74, 0x61, 0x62, 0x6c, 0x65, 0x33, 0x00,
                        // TagsDataLength
                        0x16, 0x00, 0x00, 0x00,
                        0x16, 0x00, 0x00, 0x00,
                        0x16, 0x00, 0x00, 0x00,
                        // TagsBuffer
                        0x16, 0x00, 0x00, 0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,

                        0x16, 0x00, 0x00, 0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x02, 0x00, 0x00, 0x00,

                        0x16, 0x00, 0x00, 0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x04, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00,

                        // ColDataLength
                        0x34, 0x00, 0x00, 0x00,
                        0x34, 0x00, 0x00, 0x00,
                        0x34, 0x00, 0x00, 0x00,

                        // ColBuffer
                        0x1a, 0x00, 0x00, 0x00,
                        0x09, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

                        0x1a, 0x00, 0x00, 0x00,
                        0x05, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        0x1a, 0x00, 0x00, 0x00,
                        0x09, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

                        0x1a, 0x00, 0x00, 0x00,
                        0x05, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        0x1a, 0x00, 0x00, 0x00,
                        0x09, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

                        0x1a, 0x00, 0x00, 0x00,
                        0x05, 0x00, 0x00, 0x00,
                        0x01, 0x00, 0x00, 0x00,
                        0x00,
                        0x00,
                        0x08, 0x00, 0x00, 0x00,
                        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    },
                    ExpectRows = 3,
                },
                new TestCase
                {
                    TestName = "query",
                    IsInsert = false,
                    TaosField = null,
                    FieldCount = 14,
                    BindData = new BindData[]
                    {
                        new BindData
                        {
                            TableName = null,
                            Rows = new object[][]
                            {
                                new object[]
                                {
                                    TDengineConstant.ConvertTimestampToDateTime(1726803356466,
                                        TDenginePrecision.TSDB_TIME_PRECISION_MILLI, TimeZoneInfo.Utc),
                                    true,
                                    (sbyte)11,
                                    (short)11,
                                    (int)11,
                                    (long)11,
                                    11.2f,
                                    11.2,
                                    (byte)11,
                                    (ushort)11,
                                    (uint)11,
                                    (ulong)11,
                                    System.Text.Encoding.UTF8.GetBytes("binary1"),
                                    "nchar1",
                                },
                            }
                        },
                    },
                    ExpectData = new byte[]
                    {
                        // total Length
                        0x7c, 0x01, 0x00, 0x00,
                        // tableCount
                        0x01, 0x00, 0x00, 0x00,
                        // TagCount
                        0x00, 0x00, 0x00, 0x00,
                        // ColCount
                        0x0e, 0x00, 0x00, 0x00,
                        // TableNamesOffset
                        0x00, 0x00, 0x00, 0x00,
                        // TagsOffset
                        0x00, 0x00, 0x00, 0x00,
                        // ColOffset
                        0x1c, 0x00, 0x00, 0x00,
                        // cols
                        // col length
                        0x5c, 0x01, 0x00, 0x00,
                        //table 0 cols
                        //col 0
                        //total length
                        0x32, 0x00, 0x00, 0x00,
                        //type
                        0x08, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x01,
                        // length
                        0x1c, 0x00, 0x00, 0x00,
                        // buffer length
                        0x1c, 0x00, 0x00, 0x00,
                        0x32, 0x30, 0x32, 0x34, 0x2d, 0x30, 0x39, 0x2d, 0x32, 0x30, 0x54, 0x30, 0x33, 0x3a, 0x33, 0x35,
                        0x3a, 0x35, 0x36, 0x2e, 0x34, 0x36, 0x36, 0x30, 0x30, 0x30, 0x30, 0x5a,

                        //col 1
                        //total length
                        0x13, 0x00, 0x00, 0x00,
                        //type
                        0x01, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x01, 0x00, 0x00, 0x00,
                        0x01,

                        //col 2
                        //total length
                        0x13, 0x00, 0x00, 0x00,
                        //type
                        0x02, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x01, 0x00, 0x00, 0x00,
                        0x0b,

                        //col 3
                        //total length
                        0x14, 0x00, 0x00, 0x00,
                        //type
                        0x03, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x02, 0x00, 0x00, 0x00,
                        0x0b, 0x00,

                        //col 4
                        //total length
                        0x16, 0x00, 0x00, 0x00,
                        //type
                        0x04, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x04, 0x00, 0x00, 0x00,
                        0x0b, 0x00, 0x00, 0x00,

                        //col 5
                        //total length
                        0x1a, 0x00, 0x00, 0x00,
                        //type
                        0x05, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x08, 0x00, 0x00, 0x00,
                        0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        //col 6
                        //total length
                        0x16, 0x00, 0x00, 0x00,
                        //type
                        0x06, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x04, 0x00, 0x00, 0x00,
                        0x33, 0x33, 0x33, 0x41,

                        //col 7
                        //total length
                        0x1a, 0x00, 0x00, 0x00,
                        //type
                        0x07, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x08, 0x00, 0x00, 0x00,
                        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x26, 0x40,

                        //col 8
                        //total length
                        0x13, 0x00, 0x00, 0x00,
                        //type
                        0x0b, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x01, 0x00, 0x00, 0x00,
                        0x0b,

                        //col 9
                        //total length
                        0x14, 0x00, 0x00, 0x00,
                        //type
                        0x0c, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x02, 0x00, 0x00, 0x00,
                        0x0b, 0x00,

                        //col 10
                        //total length
                        0x16, 0x00, 0x00, 0x00,
                        //type
                        0x0d, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x04, 0x00, 0x00, 0x00,
                        0x0b, 0x00, 0x00, 0x00,

                        //col 11
                        //total length
                        0x1a, 0x00, 0x00, 0x00,
                        //type
                        0x0e, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x00,
                        // buffer length
                        0x08, 0x00, 0x00, 0x00,
                        0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                        //col 12
                        //total length
                        0x1d, 0x00, 0x00, 0x00,
                        //type
                        0x08, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x01,
                        // length
                        0x07, 0x00, 0x00, 0x00,
                        // buffer length
                        0x07, 0x00, 0x00, 0x00,
                        0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x31,

                        //col 13
                        //total length
                        0x1c, 0x00, 0x00, 0x00,
                        //type
                        0x08, 0x00, 0x00, 0x00,
                        //num
                        0x01, 0x00, 0x00, 0x00,
                        //is null
                        0x00,
                        // haveLength
                        0x01,
                        // length
                        0x06, 0x00, 0x00, 0x00,
                        // buffer length
                        0x06, 0x00, 0x00, 0x00,
                        0x6e, 0x63, 0x68, 0x61, 0x72, 0x31,
                    },
                    ExpectRows = 0,
                },
            };
            for (var index = 0; index < testCases.Length; index++)
            {
                var testCase = testCases[index];
                MockStmt.PrepareAction prepareAction =
                    (string query, out bool insert, out int count, out TaosFieldAll[] fields) =>
                    {
                        insert = testCase.IsInsert;
                        count = testCase.FieldCount == 0 ? testCase.TaosField.Length : testCase.FieldCount;
                        fields = testCase.TaosField;
                    };
                MockStmt.BindBinaryAction bindAction = (byte[] data, out int rows) =>
                {
                    // StringBuilder sb = new StringBuilder();
                    // for (int i = 0; i < data.Length; i++)
                    // {
                    //     sb.Append($"0x{data[i]:X2}");
                    //     if (i < data.Length - 1)
                    //         sb.Append(", ");
                    //     if (i % 16 == 15)
                    //         sb.AppendLine();
                    // }
                    // _output.WriteLine(sb.ToString());
                    _output.WriteLine(testCase.TestName);
                    Assert.Equal(testCase.ExpectData, data);
                    rows = testCase.ExpectRows;
                };
                var mockStmt = new MockStmt(prepareAction, bindAction);
                mockStmt.Prepare("test");
                foreach (var t in testCase.BindData)
                {
                    if (!string.IsNullOrEmpty(t.TableName))
                    {
                        mockStmt.SetTableName(t.TableName);
                    }

                    if (t.Tags != null && t.Tags.Length > 0)
                    {
                        mockStmt.SetTags(t.Tags);
                    }

                    foreach (var r in t.Rows)
                    {
                        mockStmt.BindRow(r);
                    }

                    mockStmt.AddBatch();
                }

                mockStmt.Exec();
                Assert.Equal(testCase.ExpectRows, mockStmt.Affected());
            }
        }
    }
}