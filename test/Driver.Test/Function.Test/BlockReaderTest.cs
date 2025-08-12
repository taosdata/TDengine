using System;
using System.Text;
using TDengine.Driver;
using Xunit;
using Xunit.Abstractions;

namespace Driver.Test.Function.Test
{
    public class BlockReaderTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public BlockReaderTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestAllTypeRead()
        {
            var data = new byte[]
            {
                0x01, 0x00, 0x00, 0x00,
                0xf6, 0x02, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x13, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x80,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x09, 0x08, 0x00, 0x00, 0x00,
                0x01, 0x01, 0x00, 0x00, 0x00,
                0x02, 0x01, 0x00, 0x00, 0x00,
                0x03, 0x02, 0x00, 0x00, 0x00,
                0x04, 0x04, 0x00, 0x00, 0x00,
                0x05, 0x08, 0x00, 0x00, 0x00,
                0x0b, 0x01, 0x00, 0x00, 0x00,
                0x0c, 0x02, 0x00, 0x00, 0x00,
                0x0d, 0x04, 0x00, 0x00, 0x00,
                0x0e, 0x08, 0x00, 0x00, 0x00,
                0x06, 0x04, 0x00, 0x00, 0x00,
                0x07, 0x08, 0x00, 0x00, 0x00,
                0x08, 0x16, 0x00, 0x00, 0x00,
                0x0a, 0x52, 0x00, 0x00, 0x00,
                0x10, 0x16, 0x00, 0x00, 0x00,
                0x14, 0x66, 0x00, 0x00, 0x00,
                0x11, 0x04, 0x14, 0x00, 0x10,
                0x15, 0x04, 0x08, 0x00, 0x08,
                0x0f, 0x00, 0x40, 0x00, 0x00,

                0x20, 0x00, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x08, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x08, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x2a, 0x00, 0x00, 0x00,
                0x17, 0x00, 0x00, 0x00,
                0x2e, 0x00, 0x00, 0x00,
                0x40, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x21, 0x00, 0x00, 0x00,

                0x00,
                0xca, 0x61, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0xb2, 0x65, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0x9a, 0x69, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0x82, 0x6d, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,

                0x40,
                0x01,
                0x00,
                0x00,
                0x01,

                0x40,
                0x7f,
                0x00,
                0x80,
                0x01,

                0x40,
                0xff, 0x7f,
                0x00, 0x00,
                0x00, 0x80,
                0x01, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0x7f,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x80,
                0x01, 0x00, 0x00, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x40,
                0xff,
                0x00,
                0x00,
                0x01,

                0x40,
                0xff, 0xff,
                0x00, 0x00,
                0x00, 0x00,
                0x01, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0xff,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00,

                0x40,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x40,
                0x00, 0x00, 0x00, 0x4f,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x80, 0x3f,

                0x40,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x43,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x08, 0x00, 0x00, 0x00,
                0x11, 0x00, 0x00, 0x00,
                0x06, 0x00,
                0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
                0x07, 0x00,
                0xe4, 0xb8, 0xad, 0x61, 0xe6, 0x96, 0x87,
                0x01, 0x00,
                0x31,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x16, 0x00, 0x00, 0x00,
                0x24, 0x00, 0x00, 0x00,
                0x14, 0x00,
                0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72,
                0x00, 0x00, 0x00,
                0x0c, 0x00,
                0x2d, 0x4e, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x87, 0x65, 0x00, 0x00,
                0x04, 0x00,
                0x31, 0x00, 0x00, 0x00,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x0b, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x09, 0x00,
                0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
                0x07, 0x00,
                0xe4, 0xb8, 0xad, 0x61, 0xe6, 0x96, 0x87,
                0x01, 0x00,
                0x31,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x17, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x15, 0x00,
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x59, 0x40,
                0x15, 0x00,
                0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x59, 0x40,

                0x40,
                0xff, 0xff, 0x0f, 0x63, 0x2d, 0x5e, 0xc7, 0x6b, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0xf0, 0x9c, 0xd2, 0xa1, 0x38, 0x94, 0xfa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x40,
                0xff, 0xe0, 0xf5, 0x05, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x1f, 0x0a, 0xfa, 0xff, 0xff, 0xff, 0xff,
                0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x00, 0x00, 0x00, 0x00,
                0xff, 0xff, 0xff, 0xff,
                0x0b, 0x00, 0x00, 0x00,
                0x16, 0x00, 0x00, 0x00,
                0x09, 0x00,
                0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0x62, 0x22, 0x7d,
                0x09, 0x00,
                0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0x62, 0x22, 0x7d,
                0x09, 0x00,
                0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0x62, 0x22, 0x7d,

                0x00,
            };
            // create table stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(20,4),v17 decimal(8,4)) tags (info json);
            //
            // insert into t1 using stb tags ('{"a":"b"}') values(1750324502986,true,127,32767,2147483647,9223372036854775807,255,65535,4294967295,18446744073709551615,2147483647,18446744073709551615,'binary','nchar','varbinary','point(100 100)',9999999999999999.9999,9999.9999)
            // t2 using stb tags (null) values (1750324503986,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)
            // t3  using stb tags ('{"a":"b"}') values (1750324504986,false,-128,-32768,-2147483648,-9223372036854775808,0,0,0,0,0,0,'中a文','中a文','中a文','point(100 100)',-9999999999999999.9999,-9999.9999)
            // t4  using stb tags ('{"a":"b"}') values (1750324505986,true,1,1,1,1,1,1,1,1,1,1,'1','1','1',null,1,1);
            var scales = new byte[]
            {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 0
            };
            var colTypes = new byte[]
            {
                0x09,
                0x01,
                0x02,
                0x03,
                0x04,
                0x05,
                0x0b,
                0x0c,
                0x0d,
                0x0e,
                0x06,
                0x07,
                0x08,
                0x0a,
                0x10,
                0x14,
                0x11,
                0x15,
                0x0f,
            };
            var parser = new BlockReader(0, 19, (int)TDenginePrecision.TSDB_TIME_PRECISION_MILLI, colTypes, scales);
            parser.SetBlock(data);
            var values = new object[19];
            var cols = parser.GetValues(0, values);
            var expected = new object[]
            {
                TDengineConstant.ConvertTimestampToDateTime(1750324502986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                true, // bool
                (sbyte)127, // tinyint
                (short)32767, // smallint
                2147483647, // int
                9223372036854775807L, // bigint
                (byte)255, // tinyint unsigned
                (ushort)65535, // smallint unsigned
                4294967295U, // int unsigned
                18446744073709551615UL, // bigint unsigned
                2147483647F, // float
                18446744073709551615D, // double
                Encoding.UTF8.GetBytes("binary"), // binary(20)
                "nchar", // nchar(20)
                Encoding.UTF8.GetBytes("varbinary"), // varbinary(20)
                new byte[]
                {
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40
                }, // geometry(100)
                decimal.Parse("9999999999999999.9999"), // decimal(20,4)
                decimal.Parse("9999.9999"), // decimal(8,4)
                Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), // json
            };
            var dateTimeIndex = 0;
            var boolIndex = 1;
            var tinyIntIndex = 2;
            var smallIntIndex = 3;
            var intIndex = 4;
            var bigIntIndex = 5;
            var tinyIntUnsignedIndex = 6;
            var smallIntUnsignedIndex = 7;
            var intUnsignedIndex = 8;
            var bigIntUnsignedIndex = 9;
            var floatIndex = 10;
            var doubleIndex = 11;
            var binaryIndex = 12;
            var ncharIndex = 13;
            var varbinaryIndex = 14;
            var geometryIndex = 15;
            var decimal128Index = 16;
            var decimal64Index = 17;
            var jsonIndex = 18;
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);
            var rowIndex = 0;
            // column out of range
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetDateTime(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetBoolean(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetByte(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetInt16(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetInt32(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetInt64(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetFloat(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetDecimal(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetDouble(rowIndex, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => parser.GetString(rowIndex, 100));
            // get date time
            Assert.Equal(expected[0], parser.GetDateTime(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDateTime(rowIndex, boolIndex));
            // get boolean
            Assert.Equal(expected[1], parser.GetBoolean(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetBoolean(rowIndex, dateTimeIndex));
            // test convert to byte
            Assert.Equal(expected[tinyIntUnsignedIndex], parser.GetByte(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal((byte)127, parser.GetByte(rowIndex, tinyIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, smallIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, intIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, smallIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, intUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, floatIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, doubleIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, decimal128Index));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, decimal64Index));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, jsonIndex));

            // test convert to int16
            Assert.Equal((short)255, parser.GetInt16(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal((short)127, parser.GetInt16(rowIndex, tinyIntIndex));
            Assert.Equal((short)32767, parser.GetInt16(rowIndex, smallIntIndex));
            Assert.Equal((short)9999, parser.GetInt16(rowIndex, decimal64Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, intIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, smallIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, intUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, floatIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, doubleIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, jsonIndex));
            // test convert to int32
            Assert.Equal(255, parser.GetInt32(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(127, parser.GetInt32(rowIndex, tinyIntIndex));
            Assert.Equal(32767, parser.GetInt32(rowIndex, smallIntIndex));
            Assert.Equal(9999, parser.GetInt32(rowIndex, decimal64Index));
            Assert.Equal(65535, parser.GetInt32(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(2147483647, parser.GetInt32(rowIndex, intIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, intUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, floatIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, doubleIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, jsonIndex));
            // test convert to int64
            Assert.Equal(255, parser.GetInt64(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(127, parser.GetInt64(rowIndex, tinyIntIndex));
            Assert.Equal(32767, parser.GetInt64(rowIndex, smallIntIndex));
            Assert.Equal(9999, parser.GetInt64(rowIndex, decimal64Index));
            Assert.Equal(65535, parser.GetInt64(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(2147483647, parser.GetInt64(rowIndex, intIndex));
            Assert.Equal(4294967295, parser.GetInt64(rowIndex, intUnsignedIndex));
            Assert.Equal(9223372036854775807, parser.GetInt64(rowIndex, bigIntIndex));
            // lost precision for float
            Assert.Equal((long)((float)2147483647), parser.GetInt64(rowIndex, floatIndex));
            Assert.Equal(9999999999999999, parser.GetInt64(rowIndex, decimal128Index));
            Assert.Equal(1750324502986, parser.GetInt64(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, boolIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt64(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt64(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, jsonIndex));

            // test convert to decimal
            Assert.Equal(255, parser.GetDecimal(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(127, parser.GetDecimal(rowIndex, tinyIntIndex));
            Assert.Equal(32767, parser.GetDecimal(rowIndex, smallIntIndex));
            Assert.Equal(65535, parser.GetDecimal(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(2147483647, parser.GetDecimal(rowIndex, intIndex));
            Assert.Equal(4294967295, parser.GetDecimal(rowIndex, intUnsignedIndex));
            Assert.Equal(9223372036854775807, parser.GetDecimal(rowIndex, bigIntIndex));
            // lost precision for float and double
            Assert.Equal((decimal)(float)2147483647, parser.GetDecimal(rowIndex, floatIndex));
            Assert.Equal((decimal)(double)18446744073709551615, parser.GetDecimal(rowIndex, doubleIndex));

            Assert.Equal(decimal.Parse("9999.9999"), parser.GetDecimal(rowIndex, decimal64Index));
            Assert.Equal(decimal.Parse("9999999999999999.9999"), parser.GetDecimal(rowIndex, decimal128Index));
            Assert.Equal(18446744073709551615, parser.GetDecimal(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, jsonIndex));

            // test convert to double
            Assert.Equal(255, parser.GetDouble(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(127, parser.GetDouble(rowIndex, tinyIntIndex));
            Assert.Equal(32767, parser.GetDouble(rowIndex, smallIntIndex));
            Assert.Equal(65535, parser.GetDouble(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(2147483647, parser.GetDouble(rowIndex, intIndex));
            Assert.Equal(4294967295, parser.GetDouble(rowIndex, intUnsignedIndex));
            Assert.Equal(9223372036854775807, parser.GetDouble(rowIndex, bigIntIndex));
            Assert.Equal((float)2147483647, parser.GetDouble(rowIndex, floatIndex));
            Assert.Equal((double)decimal.Parse("9999.9999"), parser.GetDouble(rowIndex, decimal64Index));
            Assert.Equal((double)decimal.Parse("9999999999999999.9999"), parser.GetDouble(rowIndex, decimal128Index));
            Assert.Equal(18446744073709551615, parser.GetDouble(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(18446744073709551615, parser.GetDouble(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, jsonIndex));

            // test convert to string
            Assert.Equal("binary", parser.GetString(rowIndex, binaryIndex));
            Assert.Equal("nchar", parser.GetString(rowIndex, ncharIndex));
            Assert.Equal("varbinary", parser.GetString(rowIndex, varbinaryIndex));
            Assert.Equal("{\"a\":\"b\"}", parser.GetString(rowIndex, jsonIndex));
            Assert.Equal("9999.9999", parser.GetString(rowIndex, decimal64Index));
            Assert.Equal("9999999999999999.9999", parser.GetString(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, tinyIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, smallIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, intIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, bigIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, tinyIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, smallIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, intUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, floatIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, dateTimeIndex));

            cols = parser.GetValues(1, values);
            expected = new object[]
            {
                TDengineConstant.ConvertTimestampToDateTime(1750324503986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                null, // bool
                null, // tinyint
                null, // smallint
                null, // int
                null, // bigint
                null, // tinyint unsigned
                null, // smallint unsigned
                null, // int unsigned
                null, // bigint unsigned
                null, // float
                null, // double
                null, // binary(20)
                null, // nchar(20)
                null, // varbinary(20)
                null, // geometry(100)
                null, // decimal(20,4)
                null, // decimal(8,4)
                null, // json
            };
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);
            rowIndex = 1;
            // get boolean
            Assert.Throws<InvalidCastException>(() => parser.GetBoolean(rowIndex, boolIndex));
            // test convert to byte
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, tinyIntUnsignedIndex));
            // test convert to Int16
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, smallIntIndex));
            // test convert to Int32
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, intIndex));
            // test convert to Int64
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, bigIntIndex));
            // test convert to decimal
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, decimal64Index));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, decimal128Index));
            // test convert to float
            Assert.Throws<InvalidCastException>(() => parser.GetFloat(rowIndex, floatIndex));
            // test convert to double
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, doubleIndex));
            // test convert to string
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, jsonIndex));

            cols = parser.GetValues(2, values);
            expected = new object[]
            {
                TDengineConstant.ConvertTimestampToDateTime(1750324504986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                false, // bool
                (sbyte)(-128), // tinyint
                (short)(-32768), // smallint
                -2147483648, // int
                -9223372036854775808L, // bigint
                (byte)0, // tinyint unsigned
                (ushort)0, // smallint unsigned
                0U, // int unsigned
                0UL, // bigint unsigned
                0F, // float
                0D, // double
                Encoding.UTF8.GetBytes("中a文"), // binary(20)
                "中a文", // nchar(20)
                Encoding.UTF8.GetBytes("中a文"), // varbinary(20)
                new byte[]
                {
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40
                }, // geometry(100)
                decimal.Parse("-9999999999999999.9999"), // decimal(20,4)
                decimal.Parse("-9999.9999"), // decimal(8,4)
                Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), // json
            };
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);
            rowIndex = 2;
            // get date time
            Assert.Equal(expected[0], parser.GetDateTime(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDateTime(rowIndex, boolIndex));
            // get boolean
            Assert.Equal(expected[1], parser.GetBoolean(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetBoolean(rowIndex, dateTimeIndex));
            // test convert to byte
            Assert.Equal(expected[tinyIntUnsignedIndex], parser.GetByte(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal((byte)0, parser.GetByte(rowIndex, smallIntUnsignedIndex));
            Assert.Equal((byte)0, parser.GetByte(rowIndex, intUnsignedIndex));
            Assert.Equal((byte)0, parser.GetByte(rowIndex, bigIntUnsignedIndex));
            Assert.Equal((byte)0, parser.GetByte(rowIndex, floatIndex));
            Assert.Equal((byte)0, parser.GetByte(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, tinyIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, smallIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, intIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, decimal128Index));
            Assert.Throws<OverflowException>(() => parser.GetByte(rowIndex, decimal64Index));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, jsonIndex));


            // test convert to int16
            Assert.Equal((short)-128, parser.GetInt16(rowIndex, tinyIntIndex));
            Assert.Equal((short)-32768, parser.GetInt16(rowIndex, smallIntIndex));
            Assert.Equal((short)-9999, parser.GetInt16(rowIndex, decimal64Index));
            Assert.Equal((short)0, parser.GetInt16(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal((short)0, parser.GetByte(rowIndex, smallIntUnsignedIndex));
            Assert.Equal((short)0, parser.GetByte(rowIndex, intUnsignedIndex));
            Assert.Equal((short)0, parser.GetByte(rowIndex, bigIntUnsignedIndex));
            Assert.Equal((short)0, parser.GetByte(rowIndex, floatIndex));
            Assert.Equal((short)0, parser.GetByte(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, intIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt16(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, jsonIndex));
            // test convert to int32
            Assert.Equal(-128, parser.GetInt32(rowIndex, tinyIntIndex));
            Assert.Equal(-32768, parser.GetInt32(rowIndex, smallIntIndex));
            Assert.Equal(-9999, parser.GetInt32(rowIndex, decimal64Index));
            Assert.Equal(-2147483648, parser.GetInt32(rowIndex, intIndex));
            Assert.Equal(0, parser.GetInt32(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt32(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt32(rowIndex, intUnsignedIndex));
            Assert.Equal(0, parser.GetInt32(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt32(rowIndex, floatIndex));
            Assert.Equal(0, parser.GetInt32(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, dateTimeIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, bigIntIndex));
            Assert.Throws<OverflowException>(() => parser.GetInt32(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, jsonIndex));
            // test convert to int64
            Assert.Equal(-128, parser.GetInt64(rowIndex, tinyIntIndex));
            Assert.Equal(-32768, parser.GetInt64(rowIndex, smallIntIndex));
            Assert.Equal(-9999, parser.GetInt64(rowIndex, decimal64Index));
            Assert.Equal(-2147483648, parser.GetInt64(rowIndex, intIndex));
            Assert.Equal(-9223372036854775808, parser.GetInt64(rowIndex, bigIntIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, intUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, floatIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, doubleIndex));
            Assert.Equal(1750324504986, parser.GetInt64(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, jsonIndex));

            // test convert to decimal
            Assert.Equal(-128, parser.GetDecimal(rowIndex, tinyIntIndex));
            Assert.Equal(-32768, parser.GetDecimal(rowIndex, smallIntIndex));
            Assert.Equal(-2147483648, parser.GetDecimal(rowIndex, intIndex));
            Assert.Equal(-9223372036854775808, parser.GetDecimal(rowIndex, bigIntIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, intUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, floatIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, doubleIndex));
            Assert.Equal(decimal.Parse("-9999.9999"), parser.GetDecimal(rowIndex, decimal64Index));
            Assert.Equal(decimal.Parse("-9999999999999999.9999"), parser.GetDecimal(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, jsonIndex));

            // test convert to double
            Assert.Equal(-128, parser.GetDouble(rowIndex, tinyIntIndex));
            Assert.Equal(-32768, parser.GetDouble(rowIndex, smallIntIndex));
            Assert.Equal(-2147483648, parser.GetDouble(rowIndex, intIndex));
            Assert.Equal(-9223372036854775808, parser.GetDouble(rowIndex, bigIntIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, intUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, floatIndex));
            Assert.Equal(0, parser.GetInt64(rowIndex, doubleIndex));
            Assert.Equal((double)decimal.Parse("-9999.9999"), parser.GetDouble(rowIndex, decimal64Index));
            Assert.Equal((double)decimal.Parse("-9999999999999999.9999"), parser.GetDouble(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, jsonIndex));

            // test convert to string
            Assert.Equal("中a文", parser.GetString(rowIndex, binaryIndex));
            Assert.Equal("中a文", parser.GetString(rowIndex, ncharIndex));
            Assert.Equal("中a文", parser.GetString(rowIndex, varbinaryIndex));
            Assert.Equal("{\"a\":\"b\"}", parser.GetString(rowIndex, jsonIndex));
            Assert.Equal("-9999.9999", parser.GetString(rowIndex, decimal64Index));
            Assert.Equal("-9999999999999999.9999", parser.GetString(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, tinyIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, smallIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, intIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, bigIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, tinyIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, smallIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, intUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, floatIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, dateTimeIndex));

            cols = parser.GetValues(3, values);
            expected = new object[]
            {
                TDengineConstant.ConvertTimestampToDateTime(1750324505986, TDenginePrecision.TSDB_TIME_PRECISION_MILLI),
                true, // bool
                (sbyte)(1), // tinyint
                (short)(1), // smallint
                1, // int
                1L, // bigint
                (byte)1, // tinyint unsigned
                (ushort)1, // smallint unsigned
                1U, // int unsigned
                1UL, // bigint unsigned
                1F, // float
                1D, // double
                Encoding.UTF8.GetBytes("1"), // binary(20)
                "1", // nchar(20)
                Encoding.UTF8.GetBytes("1"), // varbinary(20)
                null, // geometry(100)
                decimal.Parse("1.0000"), // decimal(20,4)
                decimal.Parse("1.0000"), // decimal(8,4)
                Encoding.UTF8.GetBytes("{\"a\":\"b\"}"), // json
            };
            Assert.Equal(expected.Length, cols);
            Assert.Equal(expected, values);
            rowIndex = 3;
            // get date time
            Assert.Equal(expected[0], parser.GetDateTime(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDateTime(rowIndex, boolIndex));
            // get boolean
            Assert.Equal(expected[1], parser.GetBoolean(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetBoolean(rowIndex, dateTimeIndex));
            // test convert to byte
            Assert.Equal(expected[tinyIntUnsignedIndex], parser.GetByte(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, tinyIntIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, smallIntIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, intIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, bigIntIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, smallIntUnsignedIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, intUnsignedIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, bigIntUnsignedIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, floatIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, doubleIndex));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, decimal128Index));
            Assert.Equal((byte)1, parser.GetByte(rowIndex, decimal64Index));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetByte(rowIndex, jsonIndex));

            // test convert to int16
            Assert.Equal((short)1, parser.GetInt16(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, tinyIntIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, smallIntIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, decimal64Index));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, intIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, bigIntIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, smallIntUnsignedIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, intUnsignedIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, bigIntUnsignedIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, floatIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, doubleIndex));
            Assert.Equal((short)1, parser.GetInt16(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt16(rowIndex, jsonIndex));
            // test convert to int32
            Assert.Equal(1, parser.GetInt32(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, tinyIntIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, smallIntIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, decimal64Index));
            Assert.Equal(1, parser.GetInt32(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, intIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, dateTimeIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, bigIntIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, intUnsignedIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, floatIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, doubleIndex));
            Assert.Equal(1, parser.GetInt32(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt32(rowIndex, jsonIndex));
            // test convert to int64
            Assert.Equal(1, parser.GetInt64(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, tinyIntIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, smallIntIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, decimal64Index));
            Assert.Equal(1, parser.GetInt64(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, intIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, intUnsignedIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, bigIntIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(1, parser.GetInt64(rowIndex, decimal128Index));
            Assert.Equal(1, parser.GetInt64(rowIndex, doubleIndex));
            // lost precision for float
            Assert.Equal((long)((float)1), parser.GetInt64(rowIndex, floatIndex));
            Assert.Equal(1750324505986, parser.GetInt64(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetInt64(rowIndex, jsonIndex));

            // test convert to decimal
            Assert.Equal(1, parser.GetDecimal(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(1, parser.GetDecimal(rowIndex, tinyIntIndex));
            Assert.Equal(1, parser.GetDecimal(rowIndex, smallIntIndex));
            Assert.Equal(1, parser.GetDecimal(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(1, parser.GetDecimal(rowIndex, intIndex));
            Assert.Equal(1, parser.GetDecimal(rowIndex, intUnsignedIndex));
            Assert.Equal(1, parser.GetDecimal(rowIndex, bigIntIndex));
            // lost precision for float and double
            Assert.Equal((decimal)(float)1, parser.GetDecimal(rowIndex, floatIndex));
            Assert.Equal((decimal)(double)1, parser.GetDecimal(rowIndex, doubleIndex));

            Assert.Equal(decimal.Parse("1.0000"), parser.GetDecimal(rowIndex, decimal64Index));
            Assert.Equal(decimal.Parse("1.0000"), parser.GetDecimal(rowIndex, decimal128Index));
            Assert.Equal(1, parser.GetDecimal(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDecimal(rowIndex, jsonIndex));

            // test convert to double
            Assert.Equal(1, parser.GetDouble(rowIndex, tinyIntUnsignedIndex));
            Assert.Equal(1, parser.GetDouble(rowIndex, tinyIntIndex));
            Assert.Equal(1, parser.GetDouble(rowIndex, smallIntIndex));
            Assert.Equal(1, parser.GetDouble(rowIndex, smallIntUnsignedIndex));
            Assert.Equal(1, parser.GetDouble(rowIndex, intIndex));
            Assert.Equal(1, parser.GetDouble(rowIndex, intUnsignedIndex));
            Assert.Equal(1, parser.GetDouble(rowIndex, bigIntIndex));
            Assert.Equal((float)1, parser.GetDouble(rowIndex, floatIndex));
            Assert.Equal((double)decimal.Parse("1.0000"), parser.GetDouble(rowIndex, decimal64Index));
            Assert.Equal((double)decimal.Parse("1.0000"), parser.GetDouble(rowIndex, decimal128Index));
            Assert.Equal(1, parser.GetDouble(rowIndex, bigIntUnsignedIndex));
            Assert.Equal(1, parser.GetDouble(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, dateTimeIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, binaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, ncharIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, varbinaryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetDouble(rowIndex, jsonIndex));

            // test convert to string
            Assert.Equal("1", parser.GetString(rowIndex, binaryIndex));
            Assert.Equal("1", parser.GetString(rowIndex, ncharIndex));
            Assert.Equal("1", parser.GetString(rowIndex, varbinaryIndex));
            Assert.Equal("{\"a\":\"b\"}", parser.GetString(rowIndex, jsonIndex));
            Assert.Equal("1.0000", parser.GetString(rowIndex, decimal64Index));
            Assert.Equal("1.0000", parser.GetString(rowIndex, decimal128Index));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, geometryIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, boolIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, tinyIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, smallIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, intIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, bigIntIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, tinyIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, smallIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, intUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, bigIntUnsignedIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, floatIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, doubleIndex));
            Assert.Throws<InvalidCastException>(() => parser.GetString(rowIndex, dateTimeIndex));
        }

        [Fact]
        public void TestDecimal()
        {
            // create table test_decimal (ts timestamp, d1 decimal(18,5), d2 decimal(18,0), d3 decimal(18,18), d4 decimal(38,5),d5 decimal(38,0), d6 decimal(38,38));
            // insert into test_decimal values
            // (1750324502986, 1234567890123.45678,123456789012345678,0.123456789012345678,123456789012345678901234567890123.45678,12345678901234567890123456789012345678,0.12345678901234567890123456789012345678)
            // (1750324503986, -1234567890123.45678,-123456789012345678,-0.123456789012345678,-123456789012345678901234567890123.45678,-12345678901234567890123456789012345678,-0.12345678901234567890123456789012345678)
            // (1750324504986, 1234567890123.45678,123456789012345678,0.123456789012345678,1234567890123.45678,123456789012345678,0.123456789012345678)
            // (1750324505986, -1234567890123.45678,-123456789012345678,-0.123456789012345678,-1234567890123.45678,-123456789012345678,-0.123456789012345678);
            var data = new byte[]
            {
                0x01, 0x00, 0x00, 0x00,
                0xa3, 0x01, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00,
                0x07, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x80,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

                0x09, 0x08, 0x00, 0x00, 0x00,
                0x15, 0x05, 0x12, 0x00, 0x08,
                0x15, 0x00, 0x12, 0x00, 0x08,
                0x15, 0x12, 0x12, 0x00, 0x08,
                0x11, 0x05, 0x26, 0x00, 0x10,
                0x11, 0x00, 0x26, 0x00, 0x10,
                0x11, 0x26, 0x26, 0x00, 0x10,

                0x20, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x20, 0x00, 0x00, 0x00,
                0x40, 0x00, 0x00, 0x00,
                0x40, 0x00, 0x00, 0x00,
                0x40, 0x00, 0x00, 0x00,

                0x00,
                0xca, 0x61, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0xb2, 0x65, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0x9a, 0x69, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,
                0x82, 0x6d, 0x78, 0x87, 0x97, 0x01, 0x00, 0x00,

                0x00,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe,

                0x00,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe,

                0x00,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe,

                0x00,
                0x4e, 0xf3, 0x38, 0xde, 0x50, 0x90, 0x49, 0xc4, 0x13, 0x33, 0x02, 0xf0, 0xf6, 0xb0, 0x49, 0x09,
                0xb2, 0x0c, 0xc7, 0x21, 0xaf, 0x6f, 0xb6, 0x3b, 0xec, 0xcc, 0xfd, 0x0f, 0x09, 0x4f, 0xb6, 0xf6,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,

                0x00,
                0x4e, 0xf3, 0x38, 0xde, 0x50, 0x90, 0x49, 0xc4, 0x13, 0x33, 0x02, 0xf0, 0xf6, 0xb0, 0x49, 0x09,
                0xb2, 0x0c, 0xc7, 0x21, 0xaf, 0x6f, 0xb6, 0x3b, 0xec, 0xcc, 0xfd, 0x0f, 0x09, 0x4f, 0xb6, 0xf6,
                0x4e, 0xf3, 0x30, 0xa6, 0x4b, 0x9b, 0xb6, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0xb2, 0x0c, 0xcf, 0x59, 0xb4, 0x64, 0x49, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,

                0x00,
                0x4e, 0xf3, 0x38, 0xde, 0x50, 0x90, 0x49, 0xc4, 0x13, 0x33, 0x02, 0xf0, 0xf6, 0xb0, 0x49, 0x09,
                0xb2, 0x0c, 0xc7, 0x21, 0xaf, 0x6f, 0xb6, 0x3b, 0xec, 0xcc, 0xfd, 0x0f, 0x09, 0x4f, 0xb6, 0xf6,
                0x00, 0x00, 0xe0, 0x5e, 0xdc, 0xb9, 0x92, 0xe1, 0x0e, 0x33, 0x02, 0xf0, 0xf6, 0xb0, 0x49, 0x09,
                0x00, 0x00, 0x20, 0xa1, 0x23, 0x46, 0x6d, 0x1e, 0xf1, 0xcc, 0xfd, 0x0f, 0x09, 0x4f, 0xb6, 0xf6,

                0x00,
            };
            var scales = new byte[]
            {
                0, 5, 0, 18, 5, 0, 38
            };
            var colTypes = new byte[]
            {
                0x09,
                0x15,
                0x15,
                0x15,
                0x11,
                0x11,
                0x11,
            };
            var parser = new BlockReader(0, 7, (int)TDenginePrecision.TSDB_TIME_PRECISION_MILLI, colTypes, scales);
            parser.SetBlock(data);
            var values = new object[7];
            Assert.Throws<OverflowException>(() => parser.GetValues(0, values));
            Assert.Throws<OverflowException>(() => parser.GetValues(1, values));
            Assert.Throws<OverflowException>(() => parser.GetValues(2, values));
            Assert.Throws<OverflowException>(() => parser.GetValues(3, values));
            int rowIndex = 0;
            Assert.Equal(decimal.Parse("1234567890123.45678"), parser.GetDecimal(rowIndex, 1));
            Assert.Equal(decimal.Parse("123456789012345678"), parser.GetDecimal(rowIndex, 2));
            Assert.Equal(decimal.Parse("0.123456789012345678"), parser.GetDecimal(rowIndex, 3));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 4));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 5));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 6));

            Assert.Equal("1234567890123.45678", parser.GetString(rowIndex, 1));
            Assert.Equal("123456789012345678", parser.GetString(rowIndex, 2));
            Assert.Equal("0.123456789012345678", parser.GetString(rowIndex, 3));
            Assert.Equal("123456789012345678901234567890123.45678", parser.GetString(rowIndex, 4));
            Assert.Equal("12345678901234567890123456789012345678", parser.GetString(rowIndex, 5));
            Assert.Equal("0.12345678901234567890123456789012345678", parser.GetString(rowIndex, 6));
            rowIndex = 1;
            Assert.Equal(decimal.Parse("-1234567890123.45678"), parser.GetDecimal(rowIndex, 1));
            Assert.Equal(decimal.Parse("-123456789012345678"), parser.GetDecimal(rowIndex, 2));
            Assert.Equal(decimal.Parse("-0.123456789012345678"), parser.GetDecimal(rowIndex, 3));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 4));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 5));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 6));

            Assert.Equal("-1234567890123.45678", parser.GetString(rowIndex, 1));
            Assert.Equal("-123456789012345678", parser.GetString(rowIndex, 2));
            Assert.Equal("-0.123456789012345678", parser.GetString(rowIndex, 3));
            Assert.Equal("-123456789012345678901234567890123.45678", parser.GetString(rowIndex, 4));
            Assert.Equal("-12345678901234567890123456789012345678", parser.GetString(rowIndex, 5));
            Assert.Equal("-0.12345678901234567890123456789012345678", parser.GetString(rowIndex, 6));
            rowIndex = 2;
            Assert.Equal(decimal.Parse("1234567890123.45678"), parser.GetDecimal(rowIndex, 1));
            Assert.Equal(decimal.Parse("123456789012345678"), parser.GetDecimal(rowIndex, 2));
            Assert.Equal(decimal.Parse("0.123456789012345678"), parser.GetDecimal(rowIndex, 3));
            Assert.Equal(decimal.Parse("1234567890123.45678"), parser.GetDecimal(rowIndex, 4));
            Assert.Equal(decimal.Parse("123456789012345678"), parser.GetDecimal(rowIndex, 5));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 6));

            Assert.Equal("1234567890123.45678", parser.GetString(rowIndex, 1));
            Assert.Equal("123456789012345678", parser.GetString(rowIndex, 2));
            Assert.Equal("0.123456789012345678", parser.GetString(rowIndex, 3));
            Assert.Equal("1234567890123.45678", parser.GetString(rowIndex, 4));
            Assert.Equal("123456789012345678", parser.GetString(rowIndex, 5));
            Assert.Equal("0.12345678901234567800000000000000000000", parser.GetString(rowIndex, 6));
            rowIndex = 3;
            Assert.Equal(decimal.Parse("-1234567890123.45678"), parser.GetDecimal(rowIndex, 1));
            Assert.Equal(decimal.Parse("-123456789012345678"), parser.GetDecimal(rowIndex, 2));
            Assert.Equal(decimal.Parse("-0.123456789012345678"), parser.GetDecimal(rowIndex, 3));
            Assert.Equal(decimal.Parse("-1234567890123.45678"), parser.GetDecimal(rowIndex, 4));
            Assert.Equal(decimal.Parse("-123456789012345678"), parser.GetDecimal(rowIndex, 5));
            Assert.Throws<OverflowException>(() => parser.GetDecimal(rowIndex, 6));

            Assert.Equal("-1234567890123.45678", parser.GetString(rowIndex, 1));
            Assert.Equal("-123456789012345678", parser.GetString(rowIndex, 2));
            Assert.Equal("-0.123456789012345678", parser.GetString(rowIndex, 3));
            Assert.Equal("-1234567890123.45678", parser.GetString(rowIndex, 4));
            Assert.Equal("-123456789012345678", parser.GetString(rowIndex, 5));
            Assert.Equal("-0.12345678901234567800000000000000000000", parser.GetString(rowIndex, 6));
        }
    }
}