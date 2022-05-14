package com.taosdata.jdbc.ws;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.*;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.utils.NullType;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.*;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.taosdata.jdbc.TSDBConstants.*;

public class BlockResultSet extends AbstractWSResultSet {

    public BlockResultSet(Statement statement, Transport transport, RequestFactory factory,
                          QueryResp response, String database) throws SQLException {
        super(statement, transport, factory, response, database);
    }

    @Override
    public List<List<Object>> fetchJsonData() throws ExecutionException, InterruptedException {
        Request blockRequest = factory.generateFetchBlock(queryId);
        CompletableFuture<Response> fetchFuture = transport.send(blockRequest);
        FetchBlockResp resp = (FetchBlockResp) fetchFuture.get();
        ByteBuffer buffer = resp.getBuffer();
        List<List<Object>> list = new ArrayList<>();
        if (resp.getBuffer() != null) {
            for (int i = 0; i < fields.size(); i++) {
                List<Object> col = new ArrayList<>(numOfRows);
                int type = fields.get(i).getTaosType();
                switch (type) {
                    case TSDB_DATA_TYPE_BOOL:
                    case TSDB_DATA_TYPE_TINYINT:
                    case TSDB_DATA_TYPE_UTINYINT:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.get());
                        }
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                    case TSDB_DATA_TYPE_USMALLINT:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.getShort());
                        }
                        break;
                    case TSDB_DATA_TYPE_INT:
                    case TSDB_DATA_TYPE_UINT:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.getInt());
                        }
                        break;
                    case TSDB_DATA_TYPE_BIGINT:
                    case TSDB_DATA_TYPE_UBIGINT:
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.getLong());
                        }
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.getFloat());
                        }
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.getDouble());
                        }
                        break;
                    case TSDB_DATA_TYPE_BINARY: {
                        byte[] bytes = new byte[fieldLength.get(i) - 2];
                        for (int j = 0; j < numOfRows; j++) {
                            short s = buffer.getShort();
                            buffer.get(bytes);

                            byte[] tmp = Arrays.copyOf(bytes, s);
                            if (NullType.isBinaryNull(tmp, s)) {
                                col.add(null);
                                continue;
                            }
                            col.add(tmp);
                        }
                        break;
                    }
                    case TSDB_DATA_TYPE_NCHAR:
                    case TSDB_DATA_TYPE_JSON: {
                        byte[] bytes = new byte[fieldLength.get(i) - 2];
                        for (int j = 0; j < numOfRows; j++) {
                            short s = buffer.getShort();
                            buffer.get(bytes);

                            byte[] tmp = Arrays.copyOf(bytes, s);
                            if (NullType.isNcharNull(tmp, s)) {
                                col.add(null);
                                continue;
                            }
                            col.add(tmp);
                        }
                        break;
                    }
                    default:
                        // unknown type, do nothing
                        col.add(null);
                        break;
                }
                list.add(col);
            }
        }
        return list;
    }

    private Timestamp parseTimestampColumnData(long value) {
        if (TimestampPrecision.MS == this.timestampPrecision)
            return new Timestamp(value);

        if (TimestampPrecision.US == this.timestampPrecision) {
            long epochSec = value / 1000_000L;
            long nanoAdjustment = value % 1000_000L * 1000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        if (TimestampPrecision.NS == this.timestampPrecision) {
            long epochSec = value / 1000_000_000L;
            long nanoAdjustment = value % 1000_000_000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        return null;
    }

    public Object parseValue(int columnIndex) {
        Object source = result.get(columnIndex - 1).get(rowIndex);
        if (null == source)
            return null;

        int type = fields.get(columnIndex - 1).getTaosType();
        switch (type) {
            case TSDB_DATA_TYPE_BOOL: {
                byte val = (byte) source;
                if (NullType.isBooleanNull(val)) {
                    return null;
                }
                return (val == 0x0) ? Boolean.FALSE : Boolean.TRUE;
            }
            case TSDB_DATA_TYPE_TINYINT: {
                byte val = (byte) source;
                if (NullType.isTinyIntNull(val)) {
                    return null;
                }
                return val;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                byte val = (byte) source;
                if (NullType.isUnsignedTinyIntNull(val)) {
                    return null;
                }
                return (short) val & 0xFF;
            }
            case TSDB_DATA_TYPE_SMALLINT: {
                short val = (short) source;
                if (NullType.isSmallIntNull(val)) {
                    return null;
                }
                return val;
            }
            case TSDB_DATA_TYPE_USMALLINT: {
                short val = (short) source;
                if (NullType.isUnsignedSmallIntNull(val)) {
                    return null;
                }
                return val & 0xFFFF;
            }
            case TSDB_DATA_TYPE_INT: {
                int val = (int) source;
                if (NullType.isIntNull(val)) {
                    return null;
                }
                return val;
            }
            case TSDB_DATA_TYPE_UINT: {
                int val = (int) source;
                if (NullType.isUnsignedIntNull(val)) {
                    return null;
                }
                return val & 0xFFFFFFFFL;
            }
            case TSDB_DATA_TYPE_BIGINT: {
                long val = (long) source;
                if (NullType.isBigIntNull(val)) {
                    return null;
                }
                return val;
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                long val = (long) source;
                if (NullType.isBigIntNull(val)) {
                    return null;
                }
                return parseTimestampColumnData(val);
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                long val = (long) source;
                if (NullType.isUnsignedBigIntNull(val)) {
                    return null;
                }
                BigDecimal tmp = new BigDecimal(val >>> 1).multiply(new BigDecimal(2));
                return (val & 0x1) == 0x1 ? tmp.add(new BigDecimal(1)) : tmp;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float val = (float) source;
                if (NullType.isFloatNull(val)) {
                    return null;
                }
                return val;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double val = (double) source;
                if (NullType.isDoubleNull(val)) {
                    return null;
                }
                return val;
            }
            case TSDB_DATA_TYPE_BINARY: {
                return source;
            }
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON: {
                String charset = TaosGlobalConfig.getCharset();
                try {
                    return new String((byte[]) source, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
            default:
                // unknown type, do nothing
                return null;
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
//            return new NullType().toString();
            return null;
        }
        if (value instanceof String)
            return (String) value;

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

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return false;
        }
        if (value instanceof Boolean)
            return (boolean) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
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
            case TSDB_DATA_TYPE_TIMESTAMP:
                return (((long) value) == 0L) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_UBIGINT:
                return value.equals(new BigDecimal(0)) ? Boolean.FALSE : Boolean.TRUE;

            case TSDB_DATA_TYPE_FLOAT:
                return (((float) value) == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDB_DATA_TYPE_DOUBLE: {
                return (((double) value) == 0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON: {
                if ("TRUE".compareToIgnoreCase((String) value) == 0) {
                    return Boolean.TRUE;
                } else if ("FALSE".compareToIgnoreCase((String) value) == 0) {
                    return Boolean.TRUE;
                } else {
                    throw new SQLDataException();
                }
            }
            case TSDB_DATA_TYPE_BINARY: {
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
                    return Boolean.TRUE;
                } else {
                    throw new SQLDataException();
                }
            }
        }

        return Boolean.FALSE;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        if (value instanceof Byte)
            return (byte) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
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
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Byte.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Byte.MAX_VALUE)) > 0)
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
            case TSDB_DATA_TYPE_JSON:
                return Byte.parseByte((String) value);
            case TSDB_DATA_TYPE_BINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Byte.parseByte(tmp);
            }
        }

        return 0;
    }

    private void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
                "'" + valueAsString + "' in column '" + columnIndex + "' is outside valid range for the jdbcType " + jdbcType2TaosTypeName(jdbcType));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        if (value instanceof Short)
            return (short) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        switch (taosType) {
            case TSDB_DATA_TYPE_BOOL:
                return (boolean) value ? (short) 1 : (short) 0;
            case TSDB_DATA_TYPE_TINYINT:
                return (byte) value;
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
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Short.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Short.MAX_VALUE)) > 0)
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
            case TSDB_DATA_TYPE_JSON:
                return Short.parseShort((String) value);
            case TSDB_DATA_TYPE_BINARY: {
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

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        if (value instanceof Integer)
            return (int) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
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
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Integer.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Integer.MAX_VALUE)) > 0)
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
            case TSDB_DATA_TYPE_JSON:
                return Integer.parseInt((String) value);
            case TSDB_DATA_TYPE_BINARY: {
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

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        if (value instanceof Long)
            return (long) value;
        if (value instanceof Timestamp) {
            Timestamp ts = (Timestamp) value;
            switch (this.timestampPrecision) {
                case TimestampPrecision.MS:
                default:
                    return ts.getTime();
                case TimestampPrecision.US:
                    return ts.getTime() * 1000 + ts.getNanos() / 1000 % 1000;
                case TimestampPrecision.NS:
                    return ts.getTime() * 1000_000 + ts.getNanos() % 1000_000;
            }
        }

        int taosType = fields.get(columnIndex - 1).getTaosType();
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
            case TSDB_DATA_TYPE_UINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Long.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Long.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.BIGINT);
                return tmp.longValue();
            }
            case TSDB_DATA_TYPE_TIMESTAMP:
                return ((Timestamp) value).getTime();
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
            case TSDB_DATA_TYPE_JSON:
                return Long.parseLong((String) value);
            case TSDB_DATA_TYPE_BINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Long.parseLong(tmp);
            }
        }
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        if (value instanceof Float)
            return (float) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
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
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Float.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Float.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.FLOAT);
                return tmp.floatValue();
            }
            case TSDB_DATA_TYPE_DOUBLE:{
                Double tmp = (double) value;
                if (tmp < Float.MIN_VALUE || tmp > Float.MAX_VALUE)
                    throwRangeException(value.toString(), columnIndex, Types.FLOAT);
                return Float.parseFloat(String.valueOf(tmp));
            }

            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
                return Float.parseFloat(value.toString());
            case TSDB_DATA_TYPE_BINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Float.parseFloat(tmp);
            }
        }
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        if (value instanceof Double)
            return (double) value;
        if (value instanceof Float)
            return Double.parseDouble(String.valueOf(value));

        int taosType = fields.get(columnIndex - 1).getTaosType();
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
            case TSDB_DATA_TYPE_UINT:
                return (long) value;

            case TSDB_DATA_TYPE_UBIGINT: {
                BigDecimal tmp = (BigDecimal) value;
                if (tmp.compareTo(new BigDecimal(Double.MIN_VALUE)) < 0 || tmp.compareTo(new BigDecimal(Double.MAX_VALUE)) > 0)
                    throwRangeException(value.toString(), columnIndex, Types.DOUBLE);
                return tmp.floatValue();
            }

            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
                return Double.parseDouble(value.toString());
            case TSDB_DATA_TYPE_BINARY: {
                String charset = TaosGlobalConfig.getCharset();
                String tmp;
                try {
                    tmp = new String((byte[]) value, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
                return Double.parseDouble(tmp);
            }
        }
        return 0;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
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

        return value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        if (value instanceof Timestamp)
            return new Date(((Timestamp) value).getTime());
        return Utils.parseDate(value.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        if (value instanceof Timestamp)
            return new Time(((Timestamp) value).getTime());
        Time time = null;
        try {
            time = Utils.parseTime(value.toString());
        } catch (DateTimeParseException e) {
            throw new RuntimeException(e.getMessage());
        }
        return time;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        if (value instanceof Timestamp)
            return (Timestamp) value;
        if (value instanceof Long) {
            return parseTimestampColumnData((long) value);
        }
        Timestamp ret;
        try {
            ret = Utils.parseTimestamp(value.toString());
        } catch (Exception e) {
            ret = null;
            wasNull = true;
        }
        return ret;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        wasNull = value == null;
        return value;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        int columnIndex = columnNames.indexOf(columnLabel);
        if (columnIndex == -1)
            throw new SQLException("cannot find Column in result");
        return columnIndex + 1;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        if (value instanceof BigDecimal)
            return (BigDecimal) value;


        int taosType = fields.get(columnIndex - 1).getTaosType();
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
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UINT:
                return new BigDecimal((long) value);

            case TSDB_DATA_TYPE_FLOAT:
                return BigDecimal.valueOf((float) value);
            case TSDB_DATA_TYPE_DOUBLE:
                return BigDecimal.valueOf((double) value);

            case TSDB_DATA_TYPE_TIMESTAMP:
                return new BigDecimal(((Timestamp) value).getTime());
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
                return new BigDecimal(value.toString());
            case TSDB_DATA_TYPE_BINARY: {
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

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.rowIndex == -1 && this.numOfRows != 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.rowIndex >= numOfRows && this.numOfRows != 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.rowIndex == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.numOfRows == 0)
            return false;
        return this.rowIndex == (this.numOfRows - 1);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        synchronized (this) {
            if (this.numOfRows > 0) {
                this.rowIndex = -1;
            }
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        synchronized (this) {
            if (this.numOfRows > 0) {
                this.rowIndex = this.numOfRows;
            }
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        if (this.numOfRows == 0)
            return false;

        synchronized (this) {
            this.rowIndex = 0;
        }
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.numOfRows == 0)
            return false;
        synchronized (this) {
            this.rowIndex = this.numOfRows - 1;
        }
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        int row;
        synchronized (this) {
            if (this.rowIndex < 0 || this.rowIndex >= this.numOfRows)
                return 0;
            row = this.rowIndex + 1;
        }
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean previous() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Statement getStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.statement;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }
}
