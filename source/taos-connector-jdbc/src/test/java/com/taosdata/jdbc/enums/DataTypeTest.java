package com.taosdata.jdbc.enums;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

import static com.taosdata.jdbc.TSDBConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class DataTypeTest {
    private static <T extends Throwable> T assertThrows(Class<T> expectedType, Runnable executable) {
        try {
            executable.run();
        } catch (Throwable actualThrown) {
            if (expectedType.isInstance(actualThrown)) {
                return expectedType.cast(actualThrown);
            } else {
                throw new AssertionError(
                        "Expected exception of type " + expectedType.getName() + " but got " + actualThrown.getClass().getName(),
                        actualThrown
                );
            }
        }
        throw new AssertionError("Expected exception of type " + expectedType.getName() + " but no exception was thrown");
    }

    // ===================== Parameterized Test Configuration =====================
    private final DataType targetEnum;
    private final String expectedTypeName;
    private final String expectedClassName;
    private final int expectedTaosTypeValue;
    private final int expectedJdbcTypeValue;
    private final int expectedSize;

    /**
     * Constructor for parameterized test
     */
    public DataTypeTest(DataType targetEnum, String expectedTypeName, String expectedClassName,
                            int expectedTaosTypeValue, int expectedJdbcTypeValue, int expectedSize) {
        this.targetEnum = targetEnum;
        this.expectedTypeName = expectedTypeName;
        this.expectedClassName = expectedClassName;
        this.expectedTaosTypeValue = expectedTaosTypeValue;
        this.expectedJdbcTypeValue = expectedJdbcTypeValue;
        this.expectedSize = expectedSize;
    }

    /**
     * Provide test data for all DataType enum constants
     */
    @Parameterized.Parameters
    public static Collection<Object[]> enumTestData() {
        return Arrays.asList(new Object[][]{
                {DataType.NULL, "NULL", Object.class.getName(), TSDB_DATA_TYPE_NULL, Types.NULL, 0},
                {DataType.BOOL, "BOOL", Boolean.class.getName(), TSDB_DATA_TYPE_BOOL, Types.BOOLEAN, BOOLEAN_PRECISION},
                {DataType.TINYINT, "TINYINT", Byte.class.getName(), TSDB_DATA_TYPE_TINYINT, Types.TINYINT, TINYINT_PRECISION},
                {DataType.UTINYINT, "TINYINT UNSIGNED", Short.class.getName(), TSDB_DATA_TYPE_UTINYINT, Types.SMALLINT, UNSIGNED_TINYINT_PRECISION},
                {DataType.USMALLINT, "SMALLINT UNSIGNED", Integer.class.getName(), TSDB_DATA_TYPE_USMALLINT, Types.INTEGER, UNSIGNED_SMALLINT_PRECISION},
                {DataType.SMALLINT, "SMALLINT", Short.class.getName(), TSDB_DATA_TYPE_SMALLINT, Types.SMALLINT, SMALLINT_PRECISION},
                {DataType.UINT, "INT UNSIGNED", Long.class.getName(), TSDB_DATA_TYPE_UINT, Types.BIGINT, UNSIGNED_INT_PRECISION},
                {DataType.INT, "INT", Integer.class.getName(), TSDB_DATA_TYPE_INT, Types.INTEGER, INT_PRECISION},
                {DataType.UBIGINT, "BIGINT UNSIGNED", BigInteger.class.getName(), TSDB_DATA_TYPE_UBIGINT, Types.NUMERIC, UNSIGNED_BIGINT_PRECISION},
                {DataType.BIGINT, "BIGINT", Long.class.getName(), TSDB_DATA_TYPE_BIGINT, Types.BIGINT, BIGINT_PRECISION},
                {DataType.FLOAT, "FLOAT", Float.class.getName(), TSDB_DATA_TYPE_FLOAT, Types.FLOAT, FLOAT_PRECISION},
                {DataType.DOUBLE, "DOUBLE", Double.class.getName(), TSDB_DATA_TYPE_DOUBLE, Types.DOUBLE, DOUBLE_PRECISION},
                {DataType.BINARY, "BINARY", byte[].class.getName(), TSDB_DATA_TYPE_BINARY, Types.VARCHAR, 0},
                {DataType.VARCHAR, "VARCHAR", byte[].class.getName(), TSDB_DATA_TYPE_VARCHAR, Types.VARCHAR, 0},
                {DataType.TIMESTAMP, "TIMESTAMP", Timestamp.class.getName(), TSDB_DATA_TYPE_TIMESTAMP, Types.TIMESTAMP, 0},
                {DataType.NCHAR, "NCHAR", String.class.getName(), TSDB_DATA_TYPE_NCHAR, Types.NCHAR, 0},
                {DataType.JSON, "JSON", String.class.getName(), TSDB_DATA_TYPE_JSON, Types.OTHER, 0},
                {DataType.VARBINARY, "VARBINARY", byte[].class.getName(), TSDB_DATA_TYPE_VARBINARY, Types.VARBINARY, 0},
                {DataType.DECIMAL128, "DECIMAL", BigDecimal.class.getName(), TSDB_DATA_TYPE_DECIMAL128, Types.DECIMAL, DECIMAL128_PRECISION},
                {DataType.GEOMETRY, "GEOMETRY", byte[].class.getName(), TSDB_DATA_TYPE_GEOMETRY, Types.BINARY, 0},
                {DataType.DECIMAL64, "DECIMAL", BigDecimal.class.getName(), TSDB_DATA_TYPE_DECIMAL64, Types.DECIMAL, DECIMAL64_PRECISION},
                {DataType.BLOB, "BLOB", Blob.class.getName(), TSDB_DATA_TYPE_BLOB, Types.BLOB, 0}
        });
    }

    // ===================== Getter Methods Test =====================
    @Test
    public void testGetTypeName() {
        assertEquals(expectedTypeName, targetEnum.getTypeName());
    }

    @Test
    public void testGetClassName() {
        assertEquals(expectedClassName, targetEnum.getClassName());
    }

    @Test
    public void testGetTaosTypeValue() {
        assertEquals(expectedTaosTypeValue, targetEnum.getTaosTypeValue());
    }

    @Test
    public void testGetJdbcTypeValue() {
        assertEquals(expectedJdbcTypeValue, targetEnum.getJdbcTypeValue());
    }

    @Test
    public void testGetSize() {
        assertEquals(expectedSize, targetEnum.getSize());
    }

    // ===================== getDataType Method Test =====================
    @Test
    public void getDataType_NullOrEmptyName_ReturnsNullType() {
        assertEquals(DataType.NULL, DataType.getDataType(null));
        assertEquals(DataType.NULL, DataType.getDataType(""));
        assertEquals(DataType.NULL, DataType.getDataType("   "));
    }

    @Test
    public void getDataType_ValidNameIgnoreCase_ReturnsCorrectType() {
        assertEquals(DataType.BOOL, DataType.getDataType("bool"));
        assertEquals(DataType.TINYINT, DataType.getDataType("   tinyint   "));
        assertEquals(DataType.UTINYINT, DataType.getDataType("TINYINT UNSIGNED"));
        assertEquals(DataType.TIMESTAMP, DataType.getDataType("timestamp"));
    }

    @Test
    public void getDataType_InvalidName_ReturnsNullType() {
        assertEquals(DataType.NULL, DataType.getDataType("INVALID_TYPE"));
        assertEquals(DataType.NULL, DataType.getDataType("INT UNSIGNED WRONG"));
    }

    // ===================== convertJDBC2DataType Method Test =====================
    @Test
    public void convertJDBC2DataType_ValidJdbcType_ReturnsCorrectType() throws SQLException {
        // Test first matched enum for duplicate JDBC type values
        assertEquals(DataType.UTINYINT, DataType.convertJDBC2DataType(Types.SMALLINT));
        assertEquals(DataType.USMALLINT, DataType.convertJDBC2DataType(Types.INTEGER));
        assertEquals(DataType.UINT, DataType.convertJDBC2DataType(Types.BIGINT));
        assertEquals(DataType.BINARY, DataType.convertJDBC2DataType(Types.VARCHAR));
        assertEquals(DataType.DECIMAL128, DataType.convertJDBC2DataType(Types.DECIMAL));

        // Test unique JDBC type values
        assertEquals(DataType.NULL, DataType.convertJDBC2DataType(Types.NULL));
        assertEquals(DataType.BOOL, DataType.convertJDBC2DataType(Types.BOOLEAN));
        assertEquals(DataType.TINYINT, DataType.convertJDBC2DataType(Types.TINYINT));
        assertEquals(DataType.UBIGINT, DataType.convertJDBC2DataType(Types.NUMERIC));
        assertEquals(DataType.FLOAT, DataType.convertJDBC2DataType(Types.FLOAT));
        assertEquals(DataType.DOUBLE, DataType.convertJDBC2DataType(Types.DOUBLE));
        assertEquals(DataType.TIMESTAMP, DataType.convertJDBC2DataType(Types.TIMESTAMP));
        assertEquals(DataType.NCHAR, DataType.convertJDBC2DataType(Types.NCHAR));
        assertEquals(DataType.JSON, DataType.convertJDBC2DataType(Types.OTHER));
        assertEquals(DataType.VARBINARY, DataType.convertJDBC2DataType(Types.VARBINARY));
        assertEquals(DataType.GEOMETRY, DataType.convertJDBC2DataType(Types.BINARY));
        assertEquals(DataType.BLOB, DataType.convertJDBC2DataType(Types.BLOB));
    }

    @Test(expected = SQLException.class)
    public void convertJDBC2DataType_InvalidJdbcType_ThrowsSQLException() throws SQLException {
        int invalidJdbcType = Types.CHAR;
        DataType.convertJDBC2DataType(invalidJdbcType);
    }

    // ===================== convertTaosType2DataType Method Test =====================
    @Test
    public void convertTaosType2DataType_ValidTaosType_ReturnsCorrectType() throws SQLException {
        for (DataType type : DataType.values()) {
            if (type.getTaosTypeValue() == TSDB_DATA_TYPE_BINARY) {
                continue; // Skip NULL type as it is used for invalid cases
            }
            assertEquals(type, DataType.convertTaosType2DataType(type.getTaosTypeValue()));
        }
    }

    @Test(expected = SQLException.class)
    public void convertTaosType2DataType_InvalidTaosType_ThrowsSQLException() throws SQLException {
        int invalidTaosType = -999;
        DataType.convertTaosType2DataType(invalidTaosType);
    }

    // ===================== calculateColumnSize Method Test =====================
    @Test
    public void calculateColumnSize_NullOrEmptyTypeName_ReturnsMinusOne() {
        assertEquals(-1, DataType.calculateColumnSize(null, "ms", 10));
        assertEquals(-1, DataType.calculateColumnSize("", "us", 20));
        assertEquals(-1, DataType.calculateColumnSize("   ", "ms", 30));
    }

    @Test
    public void calculateColumnSize_InvalidTypeName_ReturnsMinusOne() {
        assertEquals(-1, DataType.calculateColumnSize("INVALID_TYPE", "ms", 10));
    }

    @Test
    public void calculateColumnSize_TypeWithFixedSize_ReturnsFixedSize() {
        assertEquals(BOOLEAN_PRECISION, DataType.calculateColumnSize("BOOL", null, 0));
        assertEquals(TINYINT_PRECISION, DataType.calculateColumnSize("TINYINT", "ms", 10));
        assertEquals(UNSIGNED_TINYINT_PRECISION, DataType.calculateColumnSize("TINYINT UNSIGNED", "us", 20));
        assertEquals(FLOAT_PRECISION, DataType.calculateColumnSize("FLOAT", null, 0));
    }

    @Test
    public void calculateColumnSize_TimestampType_ReturnsPrecisionBasedSize() {
        assertEquals(TIMESTAMP_MS_PRECISION, DataType.calculateColumnSize("TIMESTAMP", "ms", 0));
        assertEquals(TIMESTAMP_US_PRECISION, DataType.calculateColumnSize("TIMESTAMP", "us", 0));
        assertEquals(TIMESTAMP_US_PRECISION, DataType.calculateColumnSize("TIMESTAMP", "unknown", 0));
    }

    @Test
    public void calculateColumnSize_VariableLengthType_ReturnsGivenLength() {
        int testLength = 100;
        assertEquals(testLength, DataType.calculateColumnSize("NCHAR", "ms", testLength));
        assertEquals(testLength, DataType.calculateColumnSize("BINARY", "us", testLength));
        assertEquals(testLength, DataType.calculateColumnSize("VARCHAR", null, testLength));
        assertEquals(testLength, DataType.calculateColumnSize("VARBINARY", "ms", testLength));
        assertEquals(testLength, DataType.calculateColumnSize("GEOMETRY", "us", testLength));
    }

    // ===================== calculateDecimalDigits Method Test =====================
    @Test
    public void calculateDecimalDigits_IntegerTypes_ReturnsZero() {
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("TINYINT"));
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("TINYINT UNSIGNED"));
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("SMALLINT"));
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("SMALLINT UNSIGNED"));
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("INT"));
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("INT UNSIGNED"));
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("BIGINT"));
        assertEquals(Integer.valueOf(0), DataType.calculateDecimalDigits("BIGINT UNSIGNED"));
    }

    @Test
    public void calculateDecimalDigits_FloatType_ReturnsFive() {
        assertEquals(Integer.valueOf(5), DataType.calculateDecimalDigits("FLOAT"));
    }

    @Test
    public void calculateDecimalDigits_DoubleType_ReturnsSixteen() {
        assertEquals(Integer.valueOf(16), DataType.calculateDecimalDigits("DOUBLE"));
    }

    @Test
    public void calculateDecimalDigits_OtherTypes_ReturnsNull() {
        assertNull(DataType.calculateDecimalDigits("NULL"));
        assertNull(DataType.calculateDecimalDigits("BOOL"));
        assertNull(DataType.calculateDecimalDigits("TIMESTAMP"));
        assertNull(DataType.calculateDecimalDigits("VARCHAR"));
        assertNull(DataType.calculateDecimalDigits("DECIMAL"));
        assertNull(DataType.calculateDecimalDigits("INVALID_TYPE"));
    }
}