// src/test/java/com/taosdata/jdbc/utils/DataTypeConverUtilTest.java
package com.taosdata.jdbc.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.enums.TimestampPrecision;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;

import static com.taosdata.jdbc.TSDBConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class DataTypeConvertUtilTest {

    @Mock
    private TDBlob mockTDBlob;
    private AutoCloseable closeable;
    private String originalCharset;

    private static final int COLUMN_INDEX = 1;
    private static final int TIMESTAMP_PRECISION_MS = TimestampPrecision.MS;

    @Test
    public void testGetBoolean() throws SQLDataException {
        // Test TINYINT
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_TINYINT, (byte) 1));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_TINYINT, (byte) 0));

        // Test UTINYINT and SMALLINT
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_UTINYINT, (short) 1));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_UTINYINT, (short) 0));
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_SMALLINT, (short) 1));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_SMALLINT, (short) 0));

        // Test USMALLINT and INT
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_USMALLINT, 1));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_USMALLINT, 0));
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_INT, 1));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_INT, 0));

        // Test UINT and BIGINT
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_UINT, 1L));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_UINT, 0L));
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_BIGINT, 1L));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_BIGINT, 0L));

        // Test TIMESTAMP
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_TIMESTAMP, Instant.ofEpochMilli(1L)));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_TIMESTAMP, Instant.ofEpochMilli(0L)));

        // Test UBIGINT
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_UBIGINT, new BigInteger("1")));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_UBIGINT, new BigInteger("0")));

        // Test FLOAT
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_FLOAT, 1.0f));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_FLOAT, 0.0f));

        // Test DOUBLE
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_DOUBLE, 1.0));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_DOUBLE, 0.0));

        // Test NCHAR
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_NCHAR, "TRUE"));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_NCHAR, "FALSE"));

        // Test invalid NCHAR
        try {
            DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_NCHAR, "INVALID");
            fail("Expected SQLDataException");
        } catch (SQLDataException e) {
            // Expected exception
        }

        // Test BINARY
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_BINARY, "TRUE".getBytes()));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_BINARY, "FALSE".getBytes()));

        // Test invalid BINARY
        try {
            DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_BINARY, "INVALID".getBytes());
            fail("Expected SQLDataException");
        } catch (SQLDataException e) {
            // Expected exception
        }
    }

    @Test
    public void testGetByte() throws SQLException {
       // BOOL
        assertEquals(1, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_BOOL, true, 1));
        assertEquals(0, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_BOOL, false, 1));

        // UTINYINT and SMALLINT
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UTINYINT, (short) 127, 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_SMALLINT, (short) -128, 1));
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UTINYINT, (short) 128, 1);
            fail("Expected SQLException for UTINYINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // USMALLINT and INT
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_USMALLINT, 127, 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_INT, -128, 1));
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_USMALLINT, 128, 1);
            fail("Expected SQLException for USMALLINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // BIGINT and UINT
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_BIGINT, 127L, 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UINT, -128L, 1));
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_BIGINT, 128L, 1);
            fail("Expected SQLException for BIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // UBIGINT
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UBIGINT, new BigInteger("127"), 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UBIGINT, new BigInteger("-128"), 1));
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UBIGINT, new BigInteger("128"), 1);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // FLOAT
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_FLOAT, 127.0f, 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_FLOAT, -128.0f, 1));
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_FLOAT, 128.0f, 1);
            fail("Expected SQLException for FLOAT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // DOUBLE
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_DOUBLE, 127.0, 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_DOUBLE, -128.0, 1));
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_DOUBLE, 128.0, 1);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_NCHAR, "127", 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_NCHAR, "-128", 1));

        // BINARY
        assertEquals((byte) 127, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_BINARY, "127".getBytes(), 1));
        assertEquals((byte) -128, DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_BINARY, "-128".getBytes(), 1));

    }

    @Test
    public void testGetShort() throws SQLException {
        // BOOL
        assertEquals((short) 1, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_BOOL, true, 1));
        assertEquals((short) 0, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_BOOL, false, 1));

        // TINYINT
        assertEquals((short) 127, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1));
        assertEquals((short) -128, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1));

        // UTINYINT
        assertEquals((short) 255, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_UTINYINT, (short) 255, 1));

        // USMALLINT and INT
        assertEquals((short) 32767, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_USMALLINT, 32767, 1));
        assertEquals((short) -32768, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_INT, -32768, 1));
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_USMALLINT, 32768, 1);
            fail("Expected SQLException for USMALLINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // BIGINT and UINT
        assertEquals((short) 32767, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_BIGINT, 32767L, 1));
        assertEquals((short) -32768, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_UINT, -32768L, 1));
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_BIGINT, 32768L, 1);
            fail("Expected SQLException for BIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // UBIGINT
        assertEquals((short) 32767, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_UBIGINT, new BigInteger("32767"), 1));
        assertEquals((short) -32768, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_UBIGINT, new BigInteger("-32768"), 1));
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_UBIGINT, new BigInteger("32768"), 1);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // FLOAT
        assertEquals((short) 32767, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_FLOAT, 32767.0f, 1));
        assertEquals((short) -32768, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_FLOAT, -32768.0f, 1));
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_FLOAT, 32768.0f, 1);
            fail("Expected SQLException for FLOAT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // DOUBLE
        assertEquals((short) 32767, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_DOUBLE, 32767.0, 1));
        assertEquals((short) -32768, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_DOUBLE, -32768.0, 1));
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_DOUBLE, 32768.0, 1);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals((short) 32767, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_NCHAR, "32767", 1));
        assertEquals((short) -32768, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_NCHAR, "-32768", 1));

        // BINARY
        assertEquals((short) 32767, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_BINARY, "32767".getBytes(), 1));
        assertEquals((short) -32768, DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_BINARY, "-32768".getBytes(), 1));
    }

    @Test
    public void testGetInt() throws SQLException {
        // BOOL
        assertEquals(1, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_BOOL, true, 1));
        assertEquals(0, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_BOOL, false, 1));

        // TINYINT
        assertEquals(127, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1));
        assertEquals(-128, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1));

        // UTINYINT and SMALLINT
        assertEquals(32767, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_UTINYINT, (short) 32767, 1));
        assertEquals(-32768, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_SMALLINT, (short) -32768, 1));

        // USMALLINT and INT
        assertEquals(2147483647, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_USMALLINT, 2147483647, 1));
        assertEquals(-2147483648, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_INT, -2147483648, 1));

        // BIGINT and UINT
        assertEquals(2147483647, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_BIGINT, 2147483647L, 1));
        assertEquals(-2147483648, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_UINT, -2147483648L, 1));
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_BIGINT, 2147483648L, 1);
            fail("Expected SQLException for BIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // UBIGINT
        assertEquals(2147483647, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_UBIGINT, new BigInteger("2147483647"), 1));
        assertEquals(-2147483648, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_UBIGINT, new BigInteger("-2147483648"), 1));
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_UBIGINT, new BigInteger("2147483648"), 1);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // FLOAT
        assertEquals(2147483647, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_FLOAT, 2147483647.0f, 1));
        assertEquals(-2147483648, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_FLOAT, -2147483648.0f, 1));

        // DOUBLE
        assertEquals(2147483647, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_DOUBLE, 2147483647.0, 1));
        assertEquals(-2147483648, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_DOUBLE, -2147483648.0, 1));
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_DOUBLE, 2147483648.0, 1);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals(2147483647, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_NCHAR, "2147483647", 1));
        assertEquals(-2147483648, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_NCHAR, "-2147483648", 1));

        // BINARY
        assertEquals(2147483647, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_BINARY, "2147483647".getBytes(), 1));
        assertEquals(-2147483648, DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_BINARY, "-2147483648".getBytes(), 1));
    }

    @Test
    public void testGetLong() throws SQLException {
        // BOOL
        assertEquals(1L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_BOOL, true, 1, TimestampPrecision.MS));
        assertEquals(0L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_BOOL, false, 1, TimestampPrecision.MS));

        // TINYINT
        assertEquals(127L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1, TimestampPrecision.MS));
        assertEquals(-128L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1, TimestampPrecision.MS));

        // UTINYINT and SMALLINT
        assertEquals(32767L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_SMALLINT, (short) 32767, 1, TimestampPrecision.MS));
        assertEquals(-32768L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_SMALLINT, (short) -32768, 1, TimestampPrecision.MS));

        // USMALLINT and INT
        assertEquals(2147483647L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_INT, 2147483647, 1, TimestampPrecision.MS));
        assertEquals(-2147483648L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_INT, -2147483648, 1, TimestampPrecision.MS));

        // UINT and BIGINT
        assertEquals(9223372036854775807L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L, 1, TimestampPrecision.MS));
        assertEquals(-9223372036854775808L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_BIGINT, -9223372036854775808L, 1, TimestampPrecision.MS));

        // UBIGINT
        assertEquals(9223372036854775807L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_UBIGINT, new BigInteger("9223372036854775807"), 1, TimestampPrecision.MS));
        try {
            DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_UBIGINT, new BigInteger("9223372036854775808"), 1, TimestampPrecision.MS);
            fail("Expected SQLException for UBIGINT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // TIMESTAMP
        Timestamp ts = new Timestamp(1000L);
        assertEquals(1000L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, ts, 1, TimestampPrecision.MS));
        assertEquals(1000000L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, ts, 1, TimestampPrecision.US));
        assertEquals(1000000000L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, ts, 1, TimestampPrecision.NS));

        // FLOAT
        assertEquals(127L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_FLOAT, 127.0f, 1, TimestampPrecision.MS));
        try {
            DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_FLOAT, Float.MAX_VALUE, 1, TimestampPrecision.MS);
            fail("Expected SQLException for FLOAT out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // DOUBLE
        assertEquals(127L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_DOUBLE, 127.0, 1, TimestampPrecision.MS));
        try {
            DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_DOUBLE, Double.MAX_VALUE, 1, TimestampPrecision.MS);
            fail("Expected SQLException for DOUBLE out of range");
        } catch (SQLException e) {
            // Expected exception
        }

        // NCHAR
        assertEquals(127L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_NCHAR, "127", 1, TimestampPrecision.MS));
        assertEquals(-128L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_NCHAR, "-128", 1, TimestampPrecision.MS));

        // BINARY
        assertEquals(127L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_BINARY, "127".getBytes(), 1, TimestampPrecision.MS));
        assertEquals(-128L, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_BINARY, "-128".getBytes(), 1, TimestampPrecision.MS));

    }

    @Test
    public void testGetFloat() throws SQLException {
        // BOOL
        assertEquals(1.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_BOOL, true, 1), 0.0f);
        assertEquals(0.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_BOOL, false, 1), 0.0f);

        // TINYINT
        assertEquals(127.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1), 0.0f);
        assertEquals(-128.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1), 0.0f);

        // UTINYINT and SMALLINT
        assertEquals(32767.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_SMALLINT, (short) 32767, 1), 0.0f);
        assertEquals(-32768.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_SMALLINT, (short) -32768, 1), 0.0f);

        // USMALLINT and INT
        assertEquals(2147483647.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_INT, 2147483647, 1), 0.0f);
        assertEquals(-2147483648.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_INT, -2147483648, 1), 0.0f);

        // UINT and BIGINT
        assertEquals(9223372036854775807.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L, 1), 0.0f);
        assertEquals(-9223372036854775808.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_BIGINT, -9223372036854775808L, 1), 0.0f);

        // UBIGINT
        assertEquals(1234567890123456789.0f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_UBIGINT, new BigInteger("1234567890123456789"), 1), 0.0f);

        // DOUBLE
        assertEquals(3.141592653589793, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_DOUBLE, 3.141592653589793, 1), 0.0001f);

        // NCHAR
        assertEquals(123.45f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_NCHAR, "123.45", 1), 0.0f);

        // BINARY
        assertEquals(123.45f, DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_BINARY, "123.45".getBytes(), 1), 0.0f);
    }

    @Test
    public void testGetDouble() throws SQLException {
        // BOOL
        assertEquals(1.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_BOOL, true, 1, TimestampPrecision.MS), 0.0);
        assertEquals(0.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_BOOL, false, 1, TimestampPrecision.MS), 0.0);

        // TINYINT
        assertEquals(127.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_TINYINT, (byte) 127, 1, TimestampPrecision.MS), 0.0);
        assertEquals(-128.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_TINYINT, (byte) -128, 1, TimestampPrecision.MS), 0.0);

        // UTINYINT and SMALLINT
        assertEquals(255.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_UTINYINT, (short) 255, 1, TimestampPrecision.MS), 0.0);
        assertEquals(32767.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_SMALLINT, (short) 32767, 1, TimestampPrecision.MS), 0.0);

        // USMALLINT and INT
        assertEquals(65535.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_USMALLINT, 65535, 1, TimestampPrecision.MS), 0.0);
        assertEquals(2147483647.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_INT, 2147483647, 1, TimestampPrecision.MS), 0.0);

        // UINT and BIGINT
        assertEquals(4294967295.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_UINT, 4294967295L, 1, TimestampPrecision.MS), 0.0);
        assertEquals(9223372036854775807.0, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L, 1, TimestampPrecision.MS), 0.0);

        // UBIGINT
        assertEquals(1.0E19, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_UBIGINT, new BigInteger("10000000000000000000"), 1, TimestampPrecision.MS), 0);

        // FLOAT
        assertEquals(3.14f, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_FLOAT, 3.14f, 1, TimestampPrecision.MS), 0.0);

        // DOUBLE
        assertEquals(3.141592653589793, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_DOUBLE, 3.141592653589793, 1, TimestampPrecision.MS), 0.0);

        // NCHAR
        assertEquals(123.456, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_NCHAR, "123.456", 1, TimestampPrecision.MS), 0.0);

        // BINARY
        assertEquals(789.012, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_BINARY, "789.012".getBytes(), 1, TimestampPrecision.MS), 0.0);
    }

    @Test
    public void testGetBytes() throws SQLException {
        // Test with byte array
        byte[] byteArray = {1, 2, 3};
        assertArrayEquals(byteArray, DataTypeConvertUtil.getBytes(byteArray));

        // Test with String
        String str = "test";
        assertArrayEquals(str.getBytes(), DataTypeConvertUtil.getBytes(str));

        // Test with Long
        long longValue = 123456789L;
        assertArrayEquals(Longs.toByteArray(longValue), DataTypeConvertUtil.getBytes(longValue));

        // Test with Integer
        int intValue = 123456;
        assertArrayEquals(Ints.toByteArray(intValue), DataTypeConvertUtil.getBytes(intValue));

        // Test with Short
        short shortValue = 12345;
        assertArrayEquals(Shorts.toByteArray(shortValue), DataTypeConvertUtil.getBytes(shortValue));

        // Test with Byte
        byte byteValue = 123;
        assertArrayEquals(new byte[]{byteValue}, DataTypeConvertUtil.getBytes(byteValue));

        // Test with Object (String representation)
        Object obj = new Object() {
            @Override
            public String toString() {
                return "object";
            }
        };
        assertArrayEquals("object".getBytes(), DataTypeConvertUtil.getBytes(obj));

    }

    @Test
    public void testGetDate() {
        // Test with Timestamp
        Instant now = Instant.now();
        Date expectedDate = DateTimeUtils.getDate(now, null);
        Date actualDate = DataTypeConvertUtil.getDate(now, null);
        assertEquals(expectedDate, actualDate);

        // Test with byte array representing a date string
        String dateString = "2023-10-10 10:10:10.123";
        try {
            byte[] dateBytes = dateString.getBytes("UTF-8");
            Date parsedDate = DataTypeConvertUtil.getDate(dateBytes, null);
            assertEquals(DateTimeUtils.parseDate(dateString, null), parsedDate);
        } catch (UnsupportedEncodingException e) {
            fail("Unexpected UnsupportedEncodingException: " + e.getMessage());
        }

        // Test with String
        Date parsedDateFromString = DataTypeConvertUtil.getDate(dateString, null);
        assertEquals(DateTimeUtils.parseDate(dateString, null), parsedDateFromString);

    }

    @Test(expected = RuntimeException.class)
    public void testGetTimeException() {
        // Test with invalid byte array (should throw RuntimeException with DateTimeParseException cause)
        byte[] invalidBytes = "invalid-time-12345".getBytes(StandardCharsets.UTF_8);
            DataTypeConvertUtil.getTime(invalidBytes, ZoneId.systemDefault());
            fail("Expected RuntimeException for invalid byte array time");
    }
    @Test
    public void testGetTime() {
        ZoneId defaultZone = ZoneId.systemDefault();
        // Test with Instant
        Instant now = Instant.now();
        Time expectedTime = DateTimeUtils.getTime(now, defaultZone);
        assertEquals(expectedTime, DataTypeConvertUtil.getTime(now, defaultZone));

        // Test with byte array representing a valid time string (UTF-8 charset)
        String timeString = "2025-01-01 12:34:56.000";
        byte[] timeBytes = timeString.getBytes(StandardCharsets.UTF_8);
        Time parsedTime = Time.valueOf("12:34:56");
        assertEquals(parsedTime, DataTypeConvertUtil.getTime(timeBytes, defaultZone));

        // Test with invalid String (should throw RuntimeException with DateTimeParseException cause)
        try {
            DataTypeConvertUtil.getTime("25:70:99", defaultZone); // 无效时间格式
            fail("Expected RuntimeException for invalid string time");
        } catch (RuntimeException e) {
        }

        // Test with empty byte array
        byte[] emptyBytes = new byte[0];
        try {
            DataTypeConvertUtil.getTime(emptyBytes, defaultZone);
            fail("Expected RuntimeException for empty byte array");
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void testGetBigDecimal() {
        // BOOL
        assertEquals(new BigDecimal(1), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_BOOL, true));
        assertEquals(new BigDecimal(0), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_BOOL, false));

        // TINYINT
        assertEquals(new BigDecimal(127), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_TINYINT, (byte) 127));
        assertEquals(new BigDecimal(-128), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_TINYINT, (byte) -128));

        // UTINYINT and SMALLINT
        assertEquals(new BigDecimal(32767), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_SMALLINT, (short) 32767));
        assertEquals(new BigDecimal(-32768), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_SMALLINT, (short) -32768));

        // USMALLINT and INT
        assertEquals(new BigDecimal(2147483647), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_INT, 2147483647));
        assertEquals(new BigDecimal(-2147483648), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_INT, -2147483648));

        // UINT and BIGINT
        assertEquals(new BigDecimal(9223372036854775807L), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L));
        assertEquals(new BigDecimal(-9223372036854775808L), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_BIGINT, -9223372036854775808L));

        // FLOAT
        assertEquals(BigDecimal.valueOf(3.14f), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_FLOAT, 3.14f));

        // DOUBLE
        assertEquals(BigDecimal.valueOf(3.141592653589793), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_DOUBLE, 3.141592653589793));

        // TIMESTAMP
        Instant now = Instant.now();
        assertEquals(new BigDecimal(now.toEpochMilli()), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_TIMESTAMP, now));

        // NCHAR
        assertEquals(new BigDecimal("123.456"), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_NCHAR, "123.456"));

        // BINARY
        assertEquals(new BigDecimal("789.012"), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_BINARY, "789.012".getBytes()));

        // JSON and VARBINARY (assuming similar to BINARY)
        assertEquals(new BigDecimal("345.678"), DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_VARBINARY, "345.678".getBytes()));
    }

    @Test
    public void testParseValue() throws SQLException {
        // BOOL
        assertEquals(Boolean.TRUE, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_BOOL, (byte) 1, false));
        assertEquals(Boolean.FALSE, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_BOOL, (byte) 0, false));

        // UTINYINT
        assertEquals((short)255, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_UTINYINT, (byte) -1, false));

        // TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BINARY, JSON, VARBINARY, GEOMETRY
        assertEquals((byte) 127, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_TINYINT, (byte) 127, false));
        assertEquals((short) 32767, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_SMALLINT, (short) 32767, false));
        assertEquals(2147483647, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_INT, 2147483647, false));
        assertEquals(9223372036854775807L, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_BIGINT, 9223372036854775807L, false));
        assertEquals(3.14f, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_FLOAT, 3.14f, false));
        assertEquals(3.141592653589793, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_DOUBLE, 3.141592653589793, false));
        assertEquals("binaryData", DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_BINARY, "binaryData", false));
        assertEquals("jsonData", DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_JSON, "jsonData", false));
        assertEquals("varbinaryData", DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_VARBINARY, "varbinaryData", false));
        assertEquals("geometryData", DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_GEOMETRY, "geometryData", false));
        assertEquals(new BigDecimal("1.2"), DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_DECIMAL128, new BigDecimal("1.2"), false));
        assertEquals(new BigDecimal("1.2"), DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_DECIMAL64, new BigDecimal("1.2"), false));

        // USMALLINT
        assertEquals(65535, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_USMALLINT, (short) -1, false));

        // UINT
        assertEquals(4294967295L, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_UINT, -1, false));

        // TIMESTAMP
        Instant now = Instant.now();
        assertEquals(now, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_TIMESTAMP, now, false));

        // UBIGINT
        assertEquals(new BigInteger(MAX_UNSIGNED_LONG), DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_UBIGINT, -1L, false));

        // NCHAR
        int[] ncharData = {65, 66, 67}; // Corresponds to "ABC"
        assertEquals("ABC", DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_NCHAR, ncharData, false));

        // Default case
        assertNull(DataTypeConvertUtil.parseValue(-1, "unknownType", false));
    }

    @Test
    public void testParseTimestampColumnData() {
        // 纳秒时间戳
        long nanos = System.nanoTime();
        Instant instantFromMilli = Instant.ofEpochSecond(nanos / 1_000_000_000, (nanos % 1_000_000_000) / 1_000_000 * 1_000_000);
        Instant instantFromMacro = Instant.ofEpochSecond(nanos / 1_000_000_000, (nanos % 1_000_000_000) / 1_000 * 1_000);
        Instant instantFromNanos = Instant.ofEpochSecond(nanos / 1_000_000_000, nanos % 1_000_000_000);

        // Test with millisecond precision
        assertEquals(instantFromMilli, DateTimeUtils.parseTimestampColumnData(nanos / 1_000_000, TimestampPrecision.MS));

        // Test with microsecond precision
        assertEquals(instantFromMacro, DateTimeUtils.parseTimestampColumnData(nanos / 1_000, TimestampPrecision.US));

        // Test with nanosecond precision
        assertEquals(instantFromNanos, DateTimeUtils.parseTimestampColumnData(nanos, TimestampPrecision.NS));
    }

    @Test
    public void testGetString() throws SQLException {
        // Test String type
        String testStr = "testString";
        assertEquals(testStr, DataTypeConvertUtil.getString(testStr));

        // Test Instant type
        Instant instant = Instant.ofEpochMilli(1620000000000L);
        String expectedTsStr = Timestamp.from(instant).toString();
        assertEquals(expectedTsStr, DataTypeConvertUtil.getString(instant));

        // Test byte[] type (UTF-8)
        System.setProperty("taos.jdbc.charset", "UTF-8");
        byte[] utf8Bytes = "UTF8测试".getBytes(StandardCharsets.UTF_8);
        assertEquals("UTF8测试", DataTypeConvertUtil.getString(utf8Bytes));

        // Test other object type
        Object obj = new Integer(12345);
        assertEquals("12345", DataTypeConvertUtil.getString(obj));
    }

    @Test(expected = RuntimeException.class)
    public void testGetString_UnsupportedCharset() throws SQLException {
        TaosGlobalConfig.setCharset("INVALID_CHARSET");
        byte[] testBytes = "test".getBytes();
        DataTypeConvertUtil.getString(testBytes);
    }

    @Test
    public void testGetDouble_WithFloatAndTimestamp() throws SQLException {
        // Test Float parameter
        float floatVal = 123.45f;
        assertEquals(floatVal, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_FLOAT, floatVal, 1, TimestampPrecision.MS), 0.0);

        // Test Instant with different precisions
        Instant instant = Instant.ofEpochMilli(1620000000123L);
        long msVal = instant.toEpochMilli();
        long usVal = msVal * 1000;
        long nsVal = msVal * 1000_000;

        assertEquals(msVal, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_TIMESTAMP, instant, 1, TimestampPrecision.MS), 0.0);
        assertEquals(usVal, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_TIMESTAMP, instant, 1, TimestampPrecision.US), 0.0);
        assertEquals(nsVal, DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_TIMESTAMP, instant, 1, TimestampPrecision.NS), 0.0);
    }

    @Test(expected = RuntimeException.class)
    public void testGetBigDecimal_InvalidJsonValue() {
        DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_JSON, "not_a_number".getBytes());
    }

    @Test(expected = RuntimeException.class)
    public void testGetBigDecimal_InvalidVarbinaryValue() {
        DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_VARBINARY, "invalid_decimal".getBytes());
    }

    @Test
    public void testParseValue_WithBlobAndVarcharAsString() throws SQLException {
        // Test BLOB type
        byte[] blobData = "blobTestData".getBytes();
        assertSame(blobData, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_BLOB, blobData, false));

        // Test VARCHAR_AS_STRING = true for BINARY type
        byte[] binaryData = "binaryAsString".getBytes(StandardCharsets.UTF_8);
        String expectedStr = new String(binaryData, StandardCharsets.UTF_8);
        assertEquals(expectedStr, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_BINARY, binaryData, true));
    }

    @Test
    public void testGetLong_WithTimestampParameter() throws SQLException {
        // Test Timestamp with MS precision
        Timestamp tsMs = new Timestamp(1620000000123L);
        assertEquals(tsMs.getTime(), DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, tsMs, 1, TimestampPrecision.MS));

        // Test Timestamp with US precision
        long expectedUs = tsMs.getTime() * 1000 + tsMs.getNanos() / 1000 % 1000;
        assertEquals(expectedUs, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, tsMs, 1, TimestampPrecision.US));

        // Test Timestamp with NS precision
        long expectedNs = tsMs.getTime() * 1000_000 + tsMs.getNanos() % 1000_000;
        assertEquals(expectedNs, DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_TIMESTAMP, tsMs, 1, TimestampPrecision.NS));
    }

    @Test(expected = NumberFormatException.class)
    public void testGetByte_Json_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_JSON, "non_byte_value".getBytes(), 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetByte_Varbinary_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_VARBINARY, "abc".getBytes(), 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetShort_Json_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_JSON, "not_a_short".getBytes(), 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetShort_Varbinary_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_VARBINARY, "xyz".getBytes(), 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetInt_Json_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_JSON, "invalid_int".getBytes(), 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetInt_Varbinary_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_VARBINARY, "123abc".getBytes(), 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetFloat_Nchar_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_NCHAR, "not_a_float", 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetFloat_Json_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getFloat(TSDB_DATA_TYPE_JSON, "float_invalid".getBytes(), 1);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetDouble_Nchar_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_NCHAR, "invalid_double", 1, TimestampPrecision.MS);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetDouble_Json_InvalidValue() throws SQLException {
        DataTypeConvertUtil.getDouble(TSDB_DATA_TYPE_JSON, "123.45.67".getBytes(), 1, TimestampPrecision.MS);
    }

    @Test
    public void testgetBytes_WithTDBlob() throws SQLException {
        byte[] blobContent = "testTDBlobContent".getBytes();
        when(mockTDBlob.length()).thenReturn((long) blobContent.length);
        when(mockTDBlob.getBytes(1, (int) mockTDBlob.length())).thenReturn(blobContent);

        byte[] result = DataTypeConvertUtil.getBytes(mockTDBlob);
        assertArrayEquals(blobContent, result);
    }

    @Test
    public void testgetBytes_WithInstant() throws SQLException {
        Instant instant = Instant.ofEpochMilli(1620000000000L);
        byte[] expectedBytes = Timestamp.from(instant).toString().getBytes();
        assertArrayEquals(expectedBytes, DataTypeConvertUtil.getBytes(instant));
    }

    @Test(expected = RuntimeException.class)
    public void testGetDate_InvalidDateString() {
        DataTypeConvertUtil.getDate("invalid-date-format", null);
    }

    @Test
    public void testParseValue_UBigInt_EdgeCase() throws SQLException {
        long minVal = Long.MIN_VALUE;
        BigInteger expected = new BigInteger(1, new byte[]{
                (byte) (minVal >>> 56),
                (byte) (minVal >>> 48),
                (byte) (minVal >>> 40),
                (byte) (minVal >>> 32),
                (byte) (minVal >>> 24),
                (byte) (minVal >>> 16),
                (byte) (minVal >>> 8),
                (byte) minVal
        });
        assertEquals(expected, DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_UBIGINT, minVal, false));
    }

    @Test
    public void testGetBoolean_Json_Varbinary() throws SQLDataException {
        System.setProperty("taos.jdbc.charset", "UTF-8");

        // Test JSON type
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_JSON, "true".getBytes()));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_JSON, "false".getBytes()));

        // Test VARBINARY type
        assertTrue(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_VARBINARY, "TRUE".getBytes()));
        assertFalse(DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_VARBINARY, "FALSE".getBytes()));

        // Test invalid JSON boolean
        try {
            DataTypeConvertUtil.getBoolean(TSDB_DATA_TYPE_JSON, "yes".getBytes());
            fail("Expected SQLDataException");
        } catch (SQLDataException e) {
            // Expected
        }
    }

    @Test
    public void testGetBigDecimal_Timestamp_Precision() {
        Instant instant = Instant.ofEpochMilli(1620000000123L);
        BigDecimal expected = new BigDecimal(instant.toEpochMilli());
        assertEquals(expected, DataTypeConvertUtil.getBigDecimal(TSDB_DATA_TYPE_TIMESTAMP, instant));
    }

    // ==================== getByte - throwRangeException Tests ====================
    @Test
    public void testGetByte_UTINYINT_OutOfByteRange() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UTINYINT, (short) 128, COLUMN_INDEX);
            fail("Expected SQLException for UTINYINT exceeding byte range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_SMALLINT_BelowByteMin() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_SMALLINT, (short) -129, COLUMN_INDEX);
            fail("Expected SQLException for SMALLINT below byte min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_USMALLINT_OutOfByteRange() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_USMALLINT, 128, COLUMN_INDEX);
            fail("Expected SQLException for USMALLINT exceeding byte range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_INT_BelowByteMin() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_INT, -129, COLUMN_INDEX);
            fail("Expected SQLException for INT below byte min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_BIGINT_OutOfByteRange() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_BIGINT, 128L, COLUMN_INDEX);
            fail("Expected SQLException for BIGINT exceeding byte range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_UINT_BelowByteMin() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UINT, -129L, COLUMN_INDEX);
            fail("Expected SQLException for UINT below byte min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_UBIGINT_OutOfByteRange() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_UBIGINT, BigInteger.valueOf(128), COLUMN_INDEX);
            fail("Expected SQLException for UBIGINT exceeding byte range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_FLOAT_OutOfByteRange() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_FLOAT, 128.0f, COLUMN_INDEX);
            fail("Expected SQLException for FLOAT exceeding byte range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    @Test
    public void testGetByte_DOUBLE_BelowByteMin() {
        try {
            DataTypeConvertUtil.getByte(TSDB_DATA_TYPE_DOUBLE, -129.0, COLUMN_INDEX);
            fail("Expected SQLException for DOUBLE below byte min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType TINYINT"));
        }
    }

    // ==================== getShort - throwRangeException Tests ====================
    @Test
    public void testGetShort_USMALLINT_OutOfShortRange() {
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_USMALLINT, 32768, COLUMN_INDEX);
            fail("Expected SQLException for USMALLINT exceeding short range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType SMALLINT"));
        }
    }

    @Test
    public void testGetShort_INT_BelowShortMin() {
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_INT, -32769, COLUMN_INDEX);
            fail("Expected SQLException for INT below short min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType SMALLINT"));
        }
    }

    @Test
    public void testGetShort_BIGINT_OutOfShortRange() {
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_BIGINT, 32768L, COLUMN_INDEX);
            fail("Expected SQLException for BIGINT exceeding short range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType SMALLINT"));
        }
    }

    @Test
    public void testGetShort_UINT_BelowShortMin() {
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_UINT, -32769L, COLUMN_INDEX);
            fail("Expected SQLException for UINT below short min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType SMALLINT"));
        }
    }

    @Test
    public void testGetShort_UBIGINT_OutOfShortRange() {
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_UBIGINT, BigInteger.valueOf(32768), COLUMN_INDEX);
            fail("Expected SQLException for UBIGINT exceeding short range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType SMALLINT"));
        }
    }

    @Test
    public void testGetShort_FLOAT_OutOfShortRange() {
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_FLOAT, 32768.0f, COLUMN_INDEX);
            fail("Expected SQLException for FLOAT exceeding short range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType SMALLINT"));
        }
    }

    @Test
    public void testGetShort_DOUBLE_BelowShortMin() {
        try {
            DataTypeConvertUtil.getShort(TSDB_DATA_TYPE_DOUBLE, -32769.0, COLUMN_INDEX);
            fail("Expected SQLException for DOUBLE below short min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for the jdbcType SMALLINT"));
        }
    }

    // ==================== getInt - throwRangeException Tests ====================
    @Test
    public void testGetInt_BIGINT_OutOfIntRange() {
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_BIGINT, 2147483648L, COLUMN_INDEX);
            fail("Expected SQLException for BIGINT exceeding int range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Test
    public void testGetInt_UINT_BelowIntMin() {
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_UINT, -2147483649L, COLUMN_INDEX);
            fail("Expected SQLException for UINT below int min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Test
    public void testGetInt_UBIGINT_OutOfIntRange() {
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_UBIGINT, BigInteger.valueOf(2147483648L), COLUMN_INDEX);
            fail("Expected SQLException for UBIGINT exceeding int range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Test
    public void testGetInt_FLOAT_OutOfIntRange() {
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_FLOAT, 2148483648.0f, COLUMN_INDEX);
            fail("Expected SQLException for FLOAT exceeding int range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Test
    public void testGetInt_DOUBLE_BelowIntMin() {
        try {
            DataTypeConvertUtil.getInt(TSDB_DATA_TYPE_DOUBLE, -2147483649.0, COLUMN_INDEX);
            fail("Expected SQLException for DOUBLE below int min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    // ==================== getLong - throwRangeException Tests ====================
    @Test
    public void testGetLong_UBIGINT_OutOfLongRange() {
        try {
            BigInteger exceedLongMax = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
            DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_UBIGINT, exceedLongMax, COLUMN_INDEX, TIMESTAMP_PRECISION_MS);
            fail("Expected SQLException for UBIGINT exceeding long range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Test
    public void testGetLong_UBIGINT_BelowLongMin() {
        try {
            BigInteger belowLongMin = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
            DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_UBIGINT, belowLongMin, COLUMN_INDEX, TIMESTAMP_PRECISION_MS);
            fail("Expected SQLException for UBIGINT below long min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Test
    public void testGetLong_FLOAT_OutOfLongRange() {
        try {
            DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_FLOAT, Float.MAX_VALUE, COLUMN_INDEX, TIMESTAMP_PRECISION_MS);
            fail("Expected SQLException for FLOAT exceeding long range");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Test
    public void testGetLong_DOUBLE_BelowLongMin() {
        try {
            DataTypeConvertUtil.getLong(TSDB_DATA_TYPE_DOUBLE, -Double.MAX_VALUE, COLUMN_INDEX, TIMESTAMP_PRECISION_MS);
            fail("Expected SQLException for DOUBLE below long min value");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("outside valid range for"));
        }
    }

    @Before
    public void setUpAdditional() {
        closeable = MockitoAnnotations.openMocks(this);
        originalCharset = StandardCharsets.UTF_8.toString();
    }

    @After
    public void tearDownAdditional() throws Exception {
        closeable.close();
        if (originalCharset != null) {
            TaosGlobalConfig.setCharset(originalCharset);
        }
    }
}

