package com.taosdata.jdbc.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.enums.TimestampPrecision;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.*;

public class DataTypeConvertUtil {
    private DataTypeConvertUtil() {
    }
    public static boolean getBoolean(int taosType, Object value) throws SQLDataException {
        switch (taosType) {
            case TSDB_DATA_TYPE_TINYINT:
                return ((byte) value == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return ((short) value == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return ((int) value == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (((long) value) == 0L) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_TIMESTAMP:
                return ((Instant) value).toEpochMilli() == 0L ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UBIGINT:
                return value.equals(BigInteger.ZERO) ? Boolean.FALSE : Boolean.TRUE;

            case TSDB_DATA_TYPE_FLOAT:
                return (((float) value) == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_DOUBLE: {
                return (((double) value) == 0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDB_DATA_TYPE_NCHAR: {
                if ("TRUE".compareToIgnoreCase((String) value) == 0) {
                    return Boolean.TRUE;
                } else if ("FALSE".compareToIgnoreCase((String) value) == 0) {
                    return Boolean.FALSE;
                } else {
                    throw new SQLDataException();
                }
            }
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                if ("TRUE".compareToIgnoreCase(tmp) == 0) {
                    return Boolean.TRUE;
                } else if ("FALSE".compareToIgnoreCase(tmp) == 0) {
                    return Boolean.FALSE;
                } else {
                    throw new SQLDataException();
                }
            }
        }
        return Boolean.FALSE;
    }

    private static void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
                "'" + valueAsString + "' in column '" + columnIndex + "' is outside valid range for the jdbcType " + jdbcType2TaosTypeName(jdbcType));
    }

    public static byte getByte(int taosType, Object value, int columnIndex) throws SQLException {
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? (byte) 1 : (byte) 0;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT: {
                short tmp = (short) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT: {
                int tmp = (int) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT: {
                long tmp = (long) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                BigInteger tmp = (BigInteger) value;
                if (tmp.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0 || tmp.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);

                return tmp.byteValue();
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (double) value;
                if (tmp < Byte.MIN_VALUE || tmp > Byte.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.TINYINT);
                return (byte) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Byte.parseByte((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, e.getMessage());
                }
                return Byte.parseByte(tmp);
            }
        }

        return 0;
    }

    public static short getShort(int taosType, Object value, int columnIndex) throws SQLException {
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? (short) 1 : (short) 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT: {
                int tmp = (int) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT: {
                long tmp = (long) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                BigInteger tmp = (BigInteger) value;
                if (tmp.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0 || tmp.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return tmp.shortValue();
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (double) value;
                if (tmp < Short.MIN_VALUE || tmp > Short.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
                return (short) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Short.parseShort((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Short.parseShort(tmp);
            }
        }
        return 0;
    }

    public static int getInt(int taosType, Object value, int columnIndex) throws SQLException {
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? 1 : 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT: {
                long tmp = (long) value;
                if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return (int) tmp;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                BigInteger tmp = (BigInteger) value;
                if (tmp.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0 || tmp.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return tmp.intValue();
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return (int) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (double) value;
                if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.INTEGER);
                return (int) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Integer.parseInt((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Integer.parseInt(tmp);
            }
        }
        return 0;
    }

    public static long getLong(int taosType, Object value, int columnIndex, int timestampPrecision) throws SQLException {
        if (value instanceof Timestamp) {
            Timestamp ts = (Timestamp) value;
            switch (timestampPrecision) {
                case TimestampPrecision.MS:
                default:
                    return ts.getTime();
                case TimestampPrecision.US:
                    return ts.getTime() * 1000 + ts.getNanos() / 1000 % 1000;
                case TimestampPrecision.NS:
                    return ts.getTime() * 1000_000 + ts.getNanos() % 1000_000;
            }
        }

        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? 1 : 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigInteger tmp = (BigInteger) value;
                if (tmp.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0 || tmp.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.BIGINT);
                return tmp.longValue();
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                Instant ts = (Instant) value;
                return DateTimeUtils.toLong(ts, timestampPrecision);
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float tmp = (float) value;
                if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.BIGINT);
                return (long) tmp;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double tmp = (Double) value;
                if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.BIGINT);
                return (long) tmp;
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Long.parseLong((String) value);
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Long.parseLong(tmp);
            }
            default:
                return 0;
        }
    }

    public static float getFloat(int taosType, Object value, int columnIndex) throws SQLException {
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? (float) 1 : (float) 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigInteger tmp = (BigInteger) value;
                return tmp.floatValue();
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                Double tmp = (Double) value;
                return tmp.floatValue();
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Float.parseFloat(value.toString());
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Float.parseFloat(tmp);
            }
            default:
                return 0;
        }
    }

    public static double getDouble(int taosType, Object value, int columnIndex, int timestampPrecision) throws SQLException {
        if (value instanceof Double)
            return (double) value;
        if (value instanceof Float)
            return ((Float) value).doubleValue();

        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? 1 : 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return (short) value;
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return (int) value;
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigInteger tmp = (BigInteger) value;
                return tmp.doubleValue();
            }

            case TSDB_DATA_TYPE_TIMESTAMP: {
                Instant ts = (Instant) value;
                return DateTimeUtils.toLong(ts, timestampPrecision);
            }

            case TSDB_DATA_TYPE_NCHAR:
                return Double.parseDouble(value.toString());
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, e.getMessage());
                }
                return Double.parseDouble(tmp);
            }
        }
        return 0;
    }

    public static byte[] getBytes(Object value) throws SQLException {
        if (value instanceof byte[])
            return (byte[]) value;
        if (value instanceof String)
            return ((String) value).getBytes();
        if (value instanceof Long)
            return Longs.toByteArray((long) value);
        if (value instanceof Integer)
            return Ints.toByteArray((int) value);
        if (value instanceof Short)
            return Shorts.toByteArray((short) value);
        if (value instanceof Byte)
            return new byte[]{(byte) value};
        if (value instanceof Instant)
            return Timestamp.from((Instant) value).toString().getBytes();
        if (value instanceof TDBlob)
            return ((TDBlob) value).getBytes(1, (int) ((TDBlob) value).length());
        return value.toString().getBytes();
    }

    public static String getString(Object value) throws SQLException {
        if (value instanceof String)
            return (String) value;
        if (value instanceof Instant)
            return Timestamp.from((Instant) value).toString();
        if (value instanceof byte[]) {
            String charset = TaosGlobalConfig.getCharset();
            try {
                return new String((byte[]) value, charset);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return value.toString();
    }

    public static Date getDate(Object value, ZoneId zoneId) {
        if (value instanceof Instant) {
            return DateTimeUtils.getDate((Instant) value, zoneId);
        }
        if (value instanceof byte[]) {
            String charset = TaosGlobalConfig.getCharset();
            String tmp;
            try {
                tmp = new String((byte[]) value, charset);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
            return DateTimeUtils.parseDate(tmp, zoneId);
        }
        return DateTimeUtils.parseDate(value.toString(), zoneId);
    }

    public static Time getTime(Object value, ZoneId zoneId) {
        if (value instanceof Instant) {
            return DateTimeUtils.getTime((Instant) value, zoneId);
        }
        String tmp = "";
        if (value instanceof byte[]) {
            String charset = TaosGlobalConfig.getCharset();
            try {
                tmp = new String((byte[]) value, charset);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        } else {
            tmp = value.toString();
        }
        Time time = null;
        try {
            time = DateTimeUtils.parseTime(tmp, zoneId);
        } catch (DateTimeParseException e) {
            throw new RuntimeException(e.getMessage());
        }
        return time;
    }

    public static BigDecimal getBigDecimal(int taosType, Object value) {
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? new BigDecimal(1) : new BigDecimal(0);
            case TSDB_DATA_TYPE_TINYINT:
                return new BigDecimal((byte) value);
            case TSDB_DATA_TYPE_UTINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
                return new BigDecimal((short) value);
            case TSDB_DATA_TYPE_USMALLINT:
            case TSDB_DATA_TYPE_INT:
                return new BigDecimal((int) value);
            case TSDB_DATA_TYPE_UINT:
            case TSDB_DATA_TYPE_BIGINT:
                return new BigDecimal((long) value);
            case TSDB_DATA_TYPE_UBIGINT:
                return new BigDecimal((BigInteger) value);

            case TSDB_DATA_TYPE_FLOAT:
                return BigDecimal.valueOf((float) value);
            case TSDB_DATA_TYPE_DOUBLE:
                return BigDecimal.valueOf((double) value);

            case TSDB_DATA_TYPE_TIMESTAMP:
                return new BigDecimal(((Instant) value).toEpochMilli());
            case TSDB_DATA_TYPE_NCHAR:
                return new BigDecimal(value.toString());
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return new BigDecimal(tmp);
            }
        }

        return new BigDecimal(0);
    }

    static public Object parseValue(int type, Object source, boolean varcharAsString) throws SQLException {
        switch (type) {
            case TSDB_DATA_TYPE_BOOL: {
                byte val = (byte) source;
                return (val == 0x0) ? Boolean.FALSE : Boolean.TRUE;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                byte val = (byte) source;
                return parseUTinyInt(val);
            }
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_FLOAT:
            case TSDB_DATA_TYPE_DOUBLE:
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_DECIMAL128:
            case TSDB_DATA_TYPE_DECIMAL64:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_TIMESTAMP:{
                return source;
            }
            case TSDB_DATA_TYPE_BINARY:{
                if (varcharAsString){
                    String charset = StandardCharsets.UTF_8.name();
                    try {
                        return new String((byte[]) source, charset);
                    } catch (UnsupportedEncodingException e) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_ENCODING, "Unsupported encoding: " + charset);
                    }
                } else {
                    return source;
                }
            }

            case TSDB_DATA_TYPE_USMALLINT: {
                short val = (short) source;
                return parseUSmallInt(val);
            }
            case TSDB_DATA_TYPE_UINT: {
                int val = (int) source;
                return parseUInteger(val);
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                long val = (long) source;
                return new BigInteger(1, new byte[]{
                        (byte) (val >>> 56),
                        (byte) (val >>> 48),
                        (byte) (val >>> 40),
                        (byte) (val >>> 32),
                        (byte) (val >>> 24),
                        (byte) (val >>> 16),
                        (byte) (val >>> 8),
                        (byte) val
                });
            }
            case TSDB_DATA_TYPE_NCHAR: {
                int[] tmp = (int[]) source;
                return new String(tmp, 0, tmp.length);
            }
            default:
                // unknown type, do nothing
                return null;
        }
    }
}
