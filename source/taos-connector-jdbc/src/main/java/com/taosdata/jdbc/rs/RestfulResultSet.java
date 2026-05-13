package com.taosdata.jdbc.rs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.JsonUtil;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @deprecated Use WebSocket connection instead.
 */
@Deprecated
public class RestfulResultSet extends AbstractResultSet {

    public static final DateTimeFormatter rfc3339Parser = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .appendOffset("+HHMM", "Z").toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
            .withChronology(IsoChronology.INSTANCE);
    private static final Pattern pattern = Pattern.compile("\\.(\\d++)");
    private final Statement statement;
    // data
    private final List<List<Object>> resultSet = new ArrayList<>();
    // meta
    private final List<String> columnNames = new ArrayList<>();
    private final List<Field> columns = new ArrayList<>();
    private final RestfulResultSetMetaData metaData;

    private volatile boolean isClosed;
    private int pos = -1;

    /**
     * 由一个result的Json构造结果集，对应执行show databases, show tables等这些语句，返回结果集，但无法获取结果集对应的meta，统一当成String处理
     *
     * @param resultJson: 包含data信息的结果集，有sql返回的结果集
     ***/
    public RestfulResultSet(String database, Statement statement, JsonNode resultJson) throws SQLException {
        this.statement = statement;

        // get column metadata
        JsonNode columnMeta = resultJson.get("column_meta");
        // get row data
        JsonNode data = resultJson.get("data");

        // parse column_meta
        parseColumnMetaNew(columnMeta);
        this.metaData = new RestfulResultSetMetaData(database, columns, false);

        if (data == null || !data.isArray() || data.size() == 0)
            return;
        // parse row data
        for (JsonNode jsonRow : data) {
            List<Object> row = Lists.newArrayListWithExpectedSize(this.metaData.getColumnCount());
            for (int colIndex = 0; colIndex < this.metaData.getColumnCount(); colIndex++) {
                row.add(parseColumnData(jsonRow, colIndex, columns.get(colIndex)));
            }
            resultSet.add(row);
        }
    }

    /***
     * use this method after TDengine-2.0.18.0 to parse column meta, restful add column_meta in resultSet
     * @Param columnMeta
     */
    private void parseColumnMetaNew(JsonNode columnMeta) {
        columnNames.clear();
        columns.clear();
        for (int colIndex = 0; colIndex < columnMeta.size(); colIndex++) {
            JsonNode col = columnMeta.get(colIndex);
            String colName = col.get(0).asText();
            String typeName = col.get(1).asText();
            DataType type = DataType.getDataType(typeName);
            int colType = type.getJdbcTypeValue();
            int colLength = col.get(2).asInt();
            columnNames.add(colName);
            columns.add(new Field(colName, colType, colLength, "", type.getTaosTypeValue(), 0, 0));
        }
    }

    private Object parseColumnData(JsonNode row, int colIndex, Field field) throws SQLException {
        int taosType = field.taosType;
        if (row.get(colIndex).isNull()) {
            return null;
        }

        switch (taosType) {
            case TSDBConstants.TSDB_DATA_TYPE_NULL:
                return null;
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return row.get(colIndex).asBoolean();
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (byte) row.get(colIndex).asInt();
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (short) row.get(colIndex).asInt();
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return row.get(colIndex).asInt();
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return row.get(colIndex).asLong();
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return row.get(colIndex).floatValue();
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return row.get(colIndex).doubleValue();
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                return parseTimestampColumnData(row, colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:

                int type = field.type;
                if (Types.BINARY == type) {
                    return row.get(colIndex).isNull() ? null : row.get(colIndex).asText().getBytes();
                } else {
                    return row.get(colIndex).isNull() ? null : row.get(colIndex).asText();
                }
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                return row.get(colIndex).isNull() ? null : row.get(colIndex).asText();
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
                // all json tag or just a json tag value
                ObjectWriter objectWriter = JsonUtil.getObjectWriter(JsonNode.class);
                JsonNode jsonNode = row.get(colIndex);
                if (jsonNode != null && !jsonNode.isNull() && (jsonNode.isTextual() || jsonNode.isObject())) {
                    try {
                        return objectWriter.writeValueAsString(jsonNode);
                    } catch (JsonProcessingException e) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, e.getMessage());
                    }
                } else {
                    return jsonNode;
                }
            default:
                return row.get(colIndex);
        }
    }

    public Timestamp parseTimestampColumnData(JsonNode row, int colIndex) throws SQLException {
        if (row.get(colIndex).isNull())
            return null;

        String value = row.get(colIndex).asText();
        int index = value.lastIndexOf(":");
        // ns timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSSSSS+0x:00
        if (index > 19) {
            // ns timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSSSSS+0x00
            value = value.substring(0, index) + value.substring(index + 1);
        }
        ZonedDateTime parse = ZonedDateTime.parse(value, rfc3339Parser);
        Matcher matcher = pattern.matcher(value);
        int len = 0;
        if (matcher.find()) {
            len = matcher.group(1).length();
        }
        if (len > 6) {
            this.timestampPrecision = TimestampPrecision.NS;
        } else if (len > 3) {
            this.timestampPrecision = TimestampPrecision.US;
        } else {
            this.timestampPrecision = TimestampPrecision.MS;
        }
        return Timestamp.from(parse.toInstant());

    }

    public static class Field {
        final String name;
        final int type;
        final int length;
        final String note;
        final int taosType;
        final int scale;
        final int precision;

        public Field(String name, int type, int length, String note, int taosType, int scale, int precision) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.note = note;
            this.taosType = taosType;
            this.scale = scale;
            this.precision = precision;
        }

        public int getTaosType() {
            return taosType;
        }
        public String getName() {return name;}

        public int getScale() {
            return scale;
        }
        public int getPrecision() {
            return precision;
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        pos++;
        return pos <= resultSet.size() - 1;
    }

    @Override
    public void close() throws SQLException {
        synchronized (RestfulResultSet.class) {
            this.isClosed = true;
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof byte[])
            return new String((byte[]) value);
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return false;
        if (value instanceof Boolean)
            return (boolean) value;
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return 0;

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
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return 0;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Short.MIN_VALUE)
            return 0;
        if (valueAsLong < Short.MIN_VALUE || valueAsLong > Short.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
        return (short) valueAsLong;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return 0;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Integer.MIN_VALUE)
            return 0;
        if (valueAsLong < Integer.MIN_VALUE || valueAsLong > Integer.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.INTEGER);
        return (int) valueAsLong;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return 0;
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
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return 0;
        if (value instanceof Float)
            return (float) value;
        if (value instanceof Double)
            return new Float((Double) value);
        if (value instanceof JsonNode)
            return ((JsonNode) value).floatValue();
        return Float.parseFloat(value.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null) {
            return 0;
        }
        if (value instanceof Double)
            return (double) value;
        if (value instanceof Float)
            return (float) value;
        if (value instanceof JsonNode)
            return ((JsonNode) value).doubleValue();
        return Double.parseDouble(value.toString());
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
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

        return value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Date(((Timestamp) value).getTime());
        return DateTimeUtils.parseDate(value.toString(), null);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Time(((Timestamp) value).getTime());
        Time time = null;
        try {
            time = DateTimeUtils.parseTime(value.toString(), null);
        } catch (DateTimeParseException ignored) {
        }
        return time;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
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
            ret = DateTimeUtils.parseTimestamp(value.toString(), null);
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
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        wasNull = value == null;
        return value;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        int columnIndex = columnNames.indexOf(columnLabel);
        if (columnIndex == -1)
            throw new SQLException("cannot find Column in resultSet");
        return columnIndex + 1;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
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
        return this.pos == -1 && this.resultSet.size() != 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.pos >= resultSet.size() && this.resultSet.size() != 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.pos == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.resultSet.size() == 0)
            return false;
        return this.pos == (this.resultSet.size() - 1);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        synchronized (this) {
            if (this.resultSet.size() > 0) {
                this.pos = -1;
            }
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        synchronized (this) {
            if (this.resultSet.size() > 0) {
                this.pos = this.resultSet.size();
            }
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        if (this.resultSet.size() == 0)
            return false;

        synchronized (this) {
            this.pos = 0;
        }
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.resultSet.size() == 0)
            return false;
        synchronized (this) {
            this.pos = this.resultSet.size() - 1;
        }
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        int row;
        synchronized (this) {
            if (this.pos < 0 || this.pos >= this.resultSet.size())
                return 0;
            row = this.pos + 1;
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
        // TODO：did not use the specified timezone in cal
        return getTimestamp(columnIndex);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

}
