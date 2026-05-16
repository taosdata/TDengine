using System;
using System.Collections.Generic;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Function.Test
{
    public class TDengineConstantTypeMappingTest
    {
        public static IEnumerable<object[]> ScanTypeCases()
        {
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BOOL, typeof(bool) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_TINYINT, typeof(sbyte) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_SMALLINT, typeof(short) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_INT, typeof(int) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BIGINT, typeof(long) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_UTINYINT, typeof(byte) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_USMALLINT, typeof(ushort) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_UINT, typeof(uint) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_UBIGINT, typeof(ulong) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_FLOAT, typeof(float) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_DOUBLE, typeof(double) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BINARY, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP, typeof(DateTime) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_NCHAR, typeof(string) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_JSONTAG, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_VARBINARY, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_GEOMETRY, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BLOB, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_DECIMAL64, typeof(decimal) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_DECIMAL, typeof(decimal) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_NULL, typeof(DBNull) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_MEDIUMBLOB, typeof(DBNull) };
        }

        public static IEnumerable<object[]> ScanNullableTypeCases()
        {
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BOOL, typeof(bool?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_TINYINT, typeof(sbyte?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_SMALLINT, typeof(short?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_INT, typeof(int?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BIGINT, typeof(long?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_UTINYINT, typeof(byte?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_USMALLINT, typeof(ushort?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_UINT, typeof(uint?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_UBIGINT, typeof(ulong?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_FLOAT, typeof(float?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_DOUBLE, typeof(double?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BINARY, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP, typeof(DateTime?) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_NCHAR, typeof(string) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_JSONTAG, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_VARBINARY, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_GEOMETRY, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_BLOB, typeof(byte[]) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_DECIMAL64, typeof(string) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_DECIMAL, typeof(string) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_NULL, typeof(DBNull) };
            yield return new object[] { TDengineDataType.TSDB_DATA_TYPE_MEDIUMBLOB, typeof(DBNull) };
        }

        [Theory]
        [MemberData(nameof(ScanTypeCases))]
        public void TestScanTypeMapping(TDengineDataType dataType, Type expectedType)
        {
            var actualType = TDengineConstant.ScanType((sbyte)dataType);
            Assert.Equal(expectedType, actualType);
        }

        [Theory]
        [MemberData(nameof(ScanNullableTypeCases))]
        public void TestScanNullableTypeMapping(TDengineDataType dataType, Type expectedType)
        {
            var actualType = TDengineConstant.ScanNullableType((sbyte)dataType);
            Assert.Equal(expectedType, actualType);
        }
    }
}
