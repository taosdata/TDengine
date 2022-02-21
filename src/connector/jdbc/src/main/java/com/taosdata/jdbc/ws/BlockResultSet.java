package com.taosdata.jdbc.ws;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.TimestampFormat;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.*;

import java.math.BigDecimal;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
    public List<List<Object>> fetchJsonData() throws SQLException, ExecutionException, InterruptedException {
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
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.get() == 1);
                        }
                        break;
                    case TSDB_DATA_TYPE_UTINYINT:
                    case TSDB_DATA_TYPE_TINYINT:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.get());
                        }
                        break;
                    case TSDB_DATA_TYPE_USMALLINT:
                    case TSDB_DATA_TYPE_SMALLINT:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.getShort());
                        }
                        break;
                    case TSDB_DATA_TYPE_UINT:
                    case TSDB_DATA_TYPE_INT:
                        for (int j = 0; j < numOfRows; j++) {
                            col.add(buffer.getInt());
                        }
                        break;
                    case TSDB_DATA_TYPE_UBIGINT:
                    case TSDB_DATA_TYPE_BIGINT:
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
                            col.add(Arrays.copyOf(bytes, s));
                        }
                        break;
                    }
                    case TSDB_DATA_TYPE_NCHAR:
                    case TSDB_DATA_TYPE_JSON: {
                        byte[] bytes = new byte[fieldLength.get(i) - 2];
                        for (int j = 0; j < numOfRows; j++) {
                            short s = buffer.getShort();
                            buffer.get(bytes);
                            col.add(new String(Arrays.copyOf(bytes, s), StandardCharsets.UTF_8));
                        }
                        break;
                    }
                    case TSDB_DATA_TYPE_TIMESTAMP: {
                        byte[] bytes = new byte[fieldLength.get(i)];
                        for (int j = 0; j < numOfRows; j++) {
                            buffer.get(bytes);
                            col.add(parseTimestampColumnData(bytes));
                        }
                        break;
                    }
                    default:
                        break;
                }
                list.add(col);
            }
        }
        return list;
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer.getLong();
    }

    private Timestamp parseTimestampColumnData(byte[] bytes) throws SQLException {
        if (bytes == null || bytes.length < 1)
            return null;
        String tsFormatUpperCase = this.statement.getConnection().getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT).toUpperCase();
        TimestampFormat timestampFormat = TimestampFormat.valueOf(tsFormatUpperCase);
        switch (timestampFormat) {
            case TIMESTAMP: {
                long value = bytesToLong(bytes);
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
            }
            case UTC: {
                String value = new String(bytes);
                if (value.lastIndexOf(":") > 19) {
                    ZonedDateTime parse = ZonedDateTime.parse(value, rfc3339Parser);
                    return Timestamp.from(parse.toInstant());
                } else {
                    long epochSec = Timestamp.valueOf(value.substring(0, 19).replace("T", " ")).getTime() / 1000;
                    int fractionalSec = Integer.parseInt(value.substring(20, value.length() - 5));
                    long nanoAdjustment;
                    if (TimestampPrecision.NS == this.timestampPrecision) {
                        // ns timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSSSSS+0x00
                        nanoAdjustment = fractionalSec;
                    } else if (TimestampPrecision.US == this.timestampPrecision) {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSS+0x00
                        nanoAdjustment = fractionalSec * 1000L;
                    } else {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSS+0x00
                        nanoAdjustment = fractionalSec * 1000_000L;
                    }
                    ZoneOffset zoneOffset = ZoneOffset.of(value.substring(value.length() - 5));
                    Instant instant = Instant.ofEpochSecond(epochSec, nanoAdjustment).atOffset(zoneOffset).toInstant();
                    return Timestamp.from(instant);
                }
            }
            case STRING:
            default: {
                String value = new String(bytes, StandardCharsets.UTF_8);
                if (TimestampPrecision.MS == this.timestampPrecision) {
                    // ms timestamp: yyyy-MM-dd HH:mm:ss.SSS
                    return Timestamp.valueOf(value);
                }
                if (TimestampPrecision.US == this.timestampPrecision) {
                    // us timestamp: yyyy-MM-dd HH:mm:ss.SSSSSS
                    long epochSec = Timestamp.valueOf(value.substring(0, 19)).getTime() / 1000;
                    long nanoAdjustment = Integer.parseInt(value.substring(20)) * 1000L;
                    return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                }
                if (TimestampPrecision.NS == this.timestampPrecision) {
                    // ms timestamp: yyyy-MM-dd HH:mm:ss.SSSSSSSSS
                    long epochSec = Timestamp.valueOf(value.substring(0, 19)).getTime() / 1000;
                    long nanoAdjustment = Integer.parseInt(value.substring(20));
                    return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                }
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_TIMESTAMP_PRECISION);
            }
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof String)
            return (String) value;
        if (value instanceof byte[])
            return new String((byte[]) value);
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return false;
        if (value instanceof Boolean)
            return (boolean) value;
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return 0;
        if (value instanceof Byte)
            return (byte) value;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Byte.MIN_VALUE)
            return 0;
        if (valueAsLong < Byte.MIN_VALUE || valueAsLong > Byte.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.TINYINT);

        return (byte) valueAsLong;
    }

    private void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
                "'" + valueAsString + "' in column '" + columnIndex + "' is outside valid range for the jdbcType " + TSDBConstants.jdbcType2TaosTypeName(jdbcType));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return 0;
        if (value instanceof Short)
            return (short) value;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Short.MIN_VALUE)
            return 0;
        if (valueAsLong < Short.MIN_VALUE || valueAsLong > Short.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
        return (short) valueAsLong;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return 0;
        if (value instanceof Integer)
            return (int) value;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Integer.MIN_VALUE)
            return 0;
        if (valueAsLong < Integer.MIN_VALUE || valueAsLong > Integer.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.INTEGER);
        return (int) valueAsLong;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return 0;
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
        long valueAsLong = 0;
        try {
            valueAsLong = Long.parseLong(value.toString());
            if (valueAsLong == Long.MIN_VALUE)
                return 0;
        } catch (NumberFormatException e) {
            throwRangeException(value.toString(), columnIndex, Types.BIGINT);
        }
        return valueAsLong;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return 0;
        if (value instanceof Float)
            return (float) value;
        if (value instanceof Double)
            return new Float((Double) value);
        return Float.parseFloat(value.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null) {
            return 0;
        }
        if (value instanceof Double || value instanceof Float)
            return (double) value;
        return Double.parseDouble(value.toString());
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return null;
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
        if (value instanceof Timestamp) {
            return Utils.formatTimestamp((Timestamp) value).getBytes();
        }

        return value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Date(((Timestamp) value).getTime());
        return Utils.parseDate(value.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Time(((Timestamp) value).getTime());
        Time time = null;
        try {
            time = Utils.parseTime(value.toString());
        } catch (DateTimeParseException ignored) {
        }
        return time;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return (Timestamp) value;
        if (value instanceof Long) {
            if (1_0000_0000_0000_0L > (long) value)
                return Timestamp.from(Instant.ofEpochMilli((long) value));
            long epochSec = (long) value / 1000_000L;
            long nanoAdjustment = (long) value % 1000_000L * 1000;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
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
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = result.get(columnIndex - 1).get(rowIndex);
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

        Object value = result.get(columnIndex - 1).get(rowIndex);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
            return new BigDecimal(Long.parseLong(value.toString()));
        if (value instanceof Double || value instanceof Float)
            return BigDecimal.valueOf(Double.parseDouble(value.toString()));
        if (value instanceof Timestamp)
            return new BigDecimal(((Timestamp) value).getTime());
        BigDecimal ret;
        try {
            ret = new BigDecimal(value.toString());
        } catch (Exception e) {
            ret = null;
        }
        return ret;
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
        //TODO：did not use the specified timezone in cal
        return getTimestamp(columnIndex);
    }
}
