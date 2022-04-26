package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.*;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.enums.TimestampFormat;
import com.taosdata.jdbc.utils.Utils;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
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

public class RestfulResultSet extends AbstractResultSet implements ResultSet {

    public static DateTimeFormatter rfc3339Parser = null;

    {
        rfc3339Parser = new DateTimeFormatterBuilder()
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
                .appendFraction(ChronoField.NANO_OF_SECOND, 2, 9, true)
                .optionalEnd()
                .appendOffset("+HH:MM", "Z").toFormatter()
                .withResolverStyle(ResolverStyle.STRICT)
                .withChronology(IsoChronology.INSTANCE);
    }

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
    public RestfulResultSet(String database, Statement statement, JSONObject resultJson) throws SQLException {
        this.statement = statement;

        // get head
        JSONArray head = resultJson.getJSONArray("head");
        // get column metadata
        JSONArray columnMeta = resultJson.getJSONArray("column_meta");
        // get row data
        JSONArray data = resultJson.getJSONArray("data");
        // get rows
        Integer rows = resultJson.getInteger("rows");

        // parse column_meta
        if (columnMeta != null) {
            parseColumnMeta_new(columnMeta);
        } else {
            parseColumnMeta_old(head, data, rows);
        }
        this.metaData = new RestfulResultSetMetaData(database, columns, this);

        if (data == null || data.isEmpty())
            return;
        // parse row data
        for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
            List<Object> row = new ArrayList<>();
            JSONArray jsonRow = data.getJSONArray(rowIndex);
            for (int colIndex = 0; colIndex < this.metaData.getColumnCount(); colIndex++) {
                row.add(parseColumnData(jsonRow, colIndex, columns.get(colIndex).taos_type));
            }
            resultSet.add(row);
        }
    }

    /***
     * use this method after TDengine-2.0.18.0 to parse column meta, restful add column_meta in resultSet
     * @Param columnMeta
     */
    private void parseColumnMeta_new(JSONArray columnMeta) throws SQLException {
        columnNames.clear();
        columns.clear();
        for (int colIndex = 0; colIndex < columnMeta.size(); colIndex++) {
            JSONArray col = columnMeta.getJSONArray(colIndex);
            String col_name = col.getString(0);
            int taos_type = col.getInteger(1);
            int col_type = TSDBConstants.taosType2JdbcType(taos_type);
            int col_length = col.getInteger(2);
            columnNames.add(col_name);
            columns.add(new Field(col_name, col_type, col_length, "", taos_type));
        }
    }

    /**
     * use this method before TDengine-2.0.18.0 to parse column meta
     */
    private void parseColumnMeta_old(JSONArray head, JSONArray data, int rows) {
        columnNames.clear();
        columns.clear();
        for (int colIndex = 0; colIndex < head.size(); colIndex++) {
            String col_name = head.getString(colIndex);
            columnNames.add(col_name);

            int col_type = Types.NULL;
            int col_length = 0;
            int taos_type = TSDBConstants.TSDB_DATA_TYPE_NULL;

            JSONArray row0Json = data.getJSONArray(0);
            if (colIndex < row0Json.size()) {
                Object value = row0Json.get(colIndex);
                if (value instanceof Boolean) {
                    col_type = Types.BOOLEAN;
                    col_length = 1;
                    taos_type = TSDBConstants.TSDB_DATA_TYPE_BOOL;
                }
                if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                    col_type = Types.BIGINT;
                    col_length = 8;
                    taos_type = TSDBConstants.TSDB_DATA_TYPE_BIGINT;
                }
                if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
                    col_type = Types.DOUBLE;
                    col_length = 8;
                    taos_type = TSDBConstants.TSDB_DATA_TYPE_DOUBLE;
                }
                if (value instanceof String) {
                    col_type = Types.NCHAR;
                    col_length = ((String) value).length();
                    taos_type = TSDBConstants.TSDB_DATA_TYPE_NCHAR;
                }
            }
            columns.add(new Field(col_name, col_type, col_length, "", taos_type));
        }
    }

    private Object parseColumnData(JSONArray row, int colIndex, int taosType) throws SQLException {
        switch (taosType) {
            case TSDBConstants.TSDB_DATA_TYPE_NULL:
                return null;
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return row.getBoolean(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return row.getByte(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return row.getShort(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return row.getInteger(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return row.getLong(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return row.getFloat(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return row.getDouble(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                return parseTimestampColumnData(row, colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return row.getString(colIndex) == null ? null : row.getString(colIndex).getBytes();
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                return row.getString(colIndex) == null ? null : row.getString(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
                //  all json tag or just a json tag value
                return row.get(colIndex) != null && (row.get(colIndex) instanceof String || row.get(colIndex) instanceof JSONObject)
                        ? JSON.toJSONString(row.get(colIndex), SerializerFeature.WriteMapNullValue)
                        : row.get(colIndex);
            default:
                return row.get(colIndex);
        }
    }

    private Timestamp parseTimestampColumnData(JSONArray row, int colIndex) throws SQLException {
        if (row.get(colIndex) == null)
            return null;
        String tsFormatUpperCase = this.statement.getConnection().getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT).toUpperCase();
        TimestampFormat timestampFormat = TimestampFormat.valueOf(tsFormatUpperCase);
        switch (timestampFormat) {
            case TIMESTAMP: {
                Long value = row.getLong(colIndex);
                //TODO: this implementation has bug if the timestamp bigger than 9999_9999_9999_9
                if (value < 1_0000_0000_0000_0L) {
                    this.timestampPrecision = TimestampPrecision.MS;
                    return new Timestamp(value);
                }
                if (value >= 1_0000_0000_0000_0L && value < 1_000_000_000_000_000_0l) {
                    this.timestampPrecision = TimestampPrecision.US;
                    long epochSec = value / 1000_000L;
                    long nanoAdjustment = value % 1000_000L * 1000L;
                    return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                }
                if (value >= 1_000_000_000_000_000_0l) {
                    this.timestampPrecision = TimestampPrecision.NS;
                    long epochSec = value / 1000_000_000L;
                    long nanoAdjustment = value % 1000_000_000L;
                    return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                }
            }
            case UTC: {
                String value = row.getString(colIndex);
                if (value.lastIndexOf(":") > 19) {
                    ZonedDateTime parse = ZonedDateTime.parse(value, rfc3339Parser);
                    long nanoAdjustment;
                    if (value.length() > 32) {
                        // ns timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSSSSS+0x:00
                        this.timestampPrecision = TimestampPrecision.NS;
                    } else if (value.length() > 29) {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSS+0x:00
                        this.timestampPrecision = TimestampPrecision.US;
                    } else {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSS+0x:00
                        this.timestampPrecision = TimestampPrecision.MS;
                    }
                    return Timestamp.from(parse.toInstant());
                } else {
                    long epochSec = Timestamp.valueOf(value.substring(0, 19).replace("T", " ")).getTime() / 1000;
                    int fractionalSec = Integer.parseInt(value.substring(20, value.length() - 5));
                    long nanoAdjustment;
                    if (value.length() > 32) {
                        // ns timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSSSSS+0x00
                        nanoAdjustment = fractionalSec;
                        this.timestampPrecision = TimestampPrecision.NS;
                    } else if (value.length() > 29) {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSS+0x00
                        nanoAdjustment = fractionalSec * 1000L;
                        this.timestampPrecision = TimestampPrecision.US;
                    } else {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSS+0x00
                        nanoAdjustment = fractionalSec * 1000_000L;
                        this.timestampPrecision = TimestampPrecision.MS;
                    }
                    ZoneOffset zoneOffset = ZoneOffset.of(value.substring(value.length() - 5));
                    Instant instant = Instant.ofEpochSecond(epochSec, nanoAdjustment).atOffset(zoneOffset).toInstant();
                    return Timestamp.from(instant);
                }
            }
            case STRING:
            default: {
                String value = row.getString(colIndex);
                int precision = Utils.guessTimestampPrecision(value);
                this.timestampPrecision = precision;

                if (precision == TimestampPrecision.MS) {
                    // ms timestamp: yyyy-MM-dd HH:mm:ss.SSS
                    return (Timestamp) row.getTimestamp(colIndex);
                }
                if (precision == TimestampPrecision.US) {
                    // us timestamp: yyyy-MM-dd HH:mm:ss.SSSSSS
                    long epochSec = Timestamp.valueOf(value.substring(0, 19)).getTime() / 1000;
                    long nanoAdjustment = Integer.parseInt(value.substring(20)) * 1000L;
                    return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                }
                if (precision == TimestampPrecision.NS) {
                    // ms timestamp: yyyy-MM-dd HH:mm:ss.SSSSSSSSS
                    long epochSec = Timestamp.valueOf(value.substring(0, 19)).getTime() / 1000;
                    long nanoAdjustment = Integer.parseInt(value.substring(20));
                    return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                }
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_TIMESTAMP_PRECISION);
            }
        }
    }

    public static class Field {
        String name;
        int type;
        int length;
        String note;
        int taos_type;

        public Field(String name, int type, int length, String note, int taos_type) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.note = note;
            this.taos_type = taos_type;
        }

        public int getTaosType() {
            return taos_type;
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
        if (value instanceof Double || value instanceof Float)
            return (double) value;
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
        return Utils.parseDate(value.toString());
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
            time = Utils.parseTime(value.toString());
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
        //TODO：did not use the specified timezone in cal
        return getTimestamp(columnIndex);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

}
