package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.DataTypeConvertUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.ws.entity.QueryResp;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.util.Calendar;

import static com.taosdata.jdbc.TSDBConstants.*;

public class BlockResultSet extends AbstractWSResultSet {
    private final ZoneId zoneId;
    private final boolean varcharAsString;

    public BlockResultSet(Statement statement, Transport transport,
                          QueryResp response, String database, ZoneId zoneId) throws SQLException {
        super(statement, transport, response, database);
        this.zoneId = zoneId;
        this.varcharAsString = transport.getConnectionParam().isVarcharAsString();
    }


    public Object parseValue(int columnIndex) throws SQLException {
        Object source = result.get(columnIndex - 1).get(rowIndex);
        if (null == source)
            return null;

        int type = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.parseValue(type, source, this.varcharAsString);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (value instanceof String)
            return (String) value;
        if (value instanceof Instant)
            return DateTimeUtils.getTimestamp((Instant) value, zoneId).toString();

        if (value instanceof byte[]) {
            // for websocket, only support utf8
            String charset = StandardCharsets.UTF_8.name();
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
        wasNull = false;
        if (value instanceof Boolean)
            return (boolean) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getBoolean(taosType, value);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Byte)
            return (byte) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getByte(taosType, value, columnIndex);
    }



    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Short)
            return (short) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getShort(taosType, value, columnIndex);

    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Integer)
            return (int) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getInt(taosType, value, columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Long)
            return (long) value;
        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getLong(taosType, value, columnIndex, this.timestampPrecision);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Float)
            return (float) value;

        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getFloat(taosType, value, columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getDouble(taosType, value, columnIndex, this.timestampPrecision);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return DataTypeConvertUtil.getBytes(value);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return DataTypeConvertUtil.getDate(value, zoneId);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return DataTypeConvertUtil.getTime(value, zoneId);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (value instanceof Instant)
            return DateTimeUtils.getTimestamp((Instant) value, zoneId);
        if (value instanceof Timestamp)
            return (Timestamp) value;
        if (value instanceof Long) {
            Instant instant = DateTimeUtils.parseTimestampColumnData((long) value, this.timestampPrecision);
            return DateTimeUtils.getTimestamp(instant, zoneId);
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
        Timestamp ret;
        try {
            ret = DateTimeUtils.parseTimestamp(tmp, zoneId);
        } catch (Exception e) {
            ret = null;
            wasNull = true;
        }
        return ret;
    }

    private Object getObjectInternal(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, fields.size());

        Object value = parseValue(columnIndex);
        wasNull = value == null;
        return value;
    }
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);

        if (value instanceof Instant){
            return DateTimeUtils.getTimestamp((Instant) value, zoneId);
        } else if (fields.get(columnIndex - 1).getTaosType() == TSDB_DATA_TYPE_BLOB) {
            return new TDBlob((byte[]) value, true);
        } else {
            return value;
        }
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);

        if (value == null) {
            return null;
        }

        return new TDBlob((byte[]) value, true);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object value = getObjectInternal(columnIndex);

        if (value == null) {
            return null;
        } else if (type.isInstance(value)) {
            return type.cast(value);
        } else {
            try {
                if (type == String.class) {
                    if (value instanceof byte[]) {
                        String charset = StandardCharsets.UTF_8.name();
                        return type.cast(new String((byte[]) value, charset));
                    }
                    return type.cast(value.toString());
                } else if (type == Integer.class && value instanceof Number) {
                    return type.cast(((Number) value).intValue());
                } else if (type == Long.class && value instanceof Number) {
                    return type.cast(((Number) value).longValue());
                } else if (type == Short.class && value instanceof Number) {
                    return type.cast(((Number) value).shortValue());
                } else if (type == Double.class && value instanceof Number) {
                    return type.cast(((Number) value).doubleValue());
                } else if (type == Float.class && value instanceof Number) {
                    return type.cast(((Number) value).floatValue());
                } else if (type == BigDecimal.class && value instanceof Number) {
                    return type.cast(new BigDecimal(value.toString()));
                } else if (type == BigInteger.class && value instanceof Number) {
                    return type.cast(new BigInteger(value.toString()));
                } else if (type == Byte.class && value instanceof Number) {
                    return type.cast(((Number) value).byteValue());
                } else if (type == LocalDateTime.class && value instanceof Instant) {
                    Instant instant = (Instant) value;
                    return type.cast(DateTimeUtils.getLocalDateTime(instant, zoneId));
                } else if (type == OffsetDateTime.class && value instanceof Instant) {
                    Instant instant = (Instant) value;
                    return type.cast(DateTimeUtils.getOffsetDateTime(instant, zoneId));
                } else if (type == ZonedDateTime.class && value instanceof Instant) {
                    Instant instant = (Instant) value;
                    return type.cast(DateTimeUtils.getZonedDateTime(instant, zoneId));
                } else if (type == Blob.class && value instanceof byte[]) {
                   return type.cast(new TDBlob((byte[]) value, true));
                } else {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TYPE_CONVERT_EXCEPTION, "Cannot convert " + value.getClass() + " to " + type);
                }
            } catch (ClassCastException | UnsupportedEncodingException e) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TYPE_CONVERT_EXCEPTION, "faild to convert " + value.getClass() + " to " + type);
            }
        }
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
        wasNull = false;
        if (value instanceof BigDecimal)
            return (BigDecimal) value;


        int taosType = fields.get(columnIndex - 1).getTaosType();
        return DataTypeConvertUtil.getBigDecimal(taosType, value);
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
