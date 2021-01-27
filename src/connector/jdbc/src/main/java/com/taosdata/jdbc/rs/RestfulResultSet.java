package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBConstants;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class RestfulResultSet implements ResultSet {
    private volatile boolean isClosed;
    private int pos = -1;

    private final String database;
    private final Statement statement;
    // data
    private ArrayList<ArrayList<Object>> resultSet = new ArrayList<>();
    // meta
    private ArrayList<String> columnNames = new ArrayList<>();
    private ArrayList<Field> columns = new ArrayList<>();
    private RestfulResultSetMetaData metaData;

    /**
     * 由一个result的Json构造结果集，对应执行show databases, show tables等这些语句，返回结果集，但无法获取结果集对应的meta，统一当成String处理
     *
     * @param resultJson: 包含data信息的结果集，有sql返回的结果集
     ***/
    public RestfulResultSet(String database, Statement statement, JSONObject resultJson) {
        this.database = database;
        this.statement = statement;
        // row data
        JSONArray data = resultJson.getJSONArray("data");
        int columnIndex = 0;
        for (; columnIndex < data.size(); columnIndex++) {
            ArrayList oneRow = new ArrayList<>();
            JSONArray one = data.getJSONArray(columnIndex);
            for (int j = 0; j < one.size(); j++) {
                oneRow.add(one.getString(j));
            }
            resultSet.add(oneRow);
        }

        // column only names
        JSONArray head = resultJson.getJSONArray("head");
        for (int i = 0; i < head.size(); i++) {
            String name = head.getString(i);
            columnNames.add(name);
            columns.add(new Field(name, "", 0, ""));
        }
        this.metaData = new RestfulResultSetMetaData(this.database, columns, this);
    }

    /**
     * 由多个resultSet的JSON构造结果集
     *
     * @param resultJson: 包含data信息的结果集，有sql返回的结果集
     * @param fieldJson:  包含多个（最多2个）meta信息的结果集，有describe xxx
     **/
    public RestfulResultSet(String database, Statement statement, JSONObject resultJson, List<JSONObject> fieldJson) {
        this(database, statement, resultJson);
        ArrayList<Field> newColumns = new ArrayList<>();

        for (Field column : columns) {
            Field field = findField(column.name, fieldJson);
            if (field != null) {
                newColumns.add(field);
            } else {
                newColumns.add(column);
            }
        }
        this.columns = newColumns;
        this.metaData = new RestfulResultSetMetaData(this.database, this.columns, this);
    }

    public Field findField(String columnName, List<JSONObject> fieldJsonList) {
        for (JSONObject fieldJSON : fieldJsonList) {
            JSONArray fieldDataJson = fieldJSON.getJSONArray("data");
            for (int i = 0; i < fieldDataJson.size(); i++) {
                JSONArray field = fieldDataJson.getJSONArray(i);
                if (columnName.equalsIgnoreCase(field.getString(0))) {
                    return new Field(field.getString(0), field.getString(1), field.getInteger(2), field.getString(3));
                }
            }
        }
        return null;
    }

    public class Field {
        String name;
        String type;
        int length;
        String note;

        public Field(String name, String type, int length, String note) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.note = note;
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        pos++;
        if (pos <= resultSet.size() - 1) {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        synchronized (RestfulResultSet.class) {
            this.isClosed = true;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        return resultSet.isEmpty();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        if (columnIndex > resultSet.get(pos).size()) {
            throw new SQLException(TSDBConstants.WrapErrMsg("Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size()));
        }

        columnIndex = getTrueColumnIndex(columnIndex);
        return resultSet.get(pos).get(columnIndex).toString();
    }


    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        columnIndex = getTrueColumnIndex(columnIndex);
        int result = getInt(columnIndex);
        return result == 0 ? false : true;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        columnIndex = getTrueColumnIndex(columnIndex);
        return Short.parseShort(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        columnIndex = getTrueColumnIndex(columnIndex);
        return Integer.parseInt(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        columnIndex = getTrueColumnIndex(columnIndex);
        return Long.parseLong(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        columnIndex = getTrueColumnIndex(columnIndex);
        return Float.parseFloat(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        columnIndex = getTrueColumnIndex(columnIndex);
        return Double.parseDouble(resultSet.get(pos).get(columnIndex).toString());
    }

    private int getTrueColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw new SQLException("Column Index out of range, " + columnIndex + " < 1");
        }

        int numOfCols = resultSet.get(pos).size();
        if (columnIndex > numOfCols) {
            throw new SQLException("Column Index out of range, " + columnIndex + " > " + numOfCols);
        }

        return columnIndex - 1;
    }

    /*******************************************************************************************************************/

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        columnIndex = getTrueColumnIndex(columnIndex);
        String strDate = resultSet.get(pos).get(columnIndex).toString();
//        strDate = strDate.substring(1, strDate.length() - 1);
        return Timestamp.valueOf(strDate);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    /*************************************************************************************************************/

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    /*************************************************************************************************************/

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        return;
    }

    @Override
    public String getCursorName() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return this.metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        int columnIndex = columnNames.indexOf(columnLabel);
        if (columnIndex == -1)
            throw new SQLException("cannot find Column in resultSet");
        return columnIndex + 1;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        return this.pos == -1 && this.resultSet.size() != 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return this.pos >= resultSet.size() && this.resultSet.size() != 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return this.pos == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        if (this.resultSet.size() == 0)
            return false;
        return this.pos == (this.resultSet.size() - 1);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        synchronized (this) {
            if (this.resultSet.size() > 0) {
                this.pos = -1;
            }
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        synchronized (this) {
            if (this.resultSet.size() > 0) {
                this.pos = this.resultSet.size();
            }
        }

    }

    @Override
    public boolean first() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

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
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
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
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
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
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

//        if (this.resultSet.size() == 0)
//            return false;
//
//        if (row == 0) {
//            beforeFirst();
//            return false;
//        } else if (row == 1) {
//            return first();
//        } else if (row == -1) {
//            return last();
//        } else if (row > this.resultSet.size()) {
//            afterLast();
//            return false;
//        } else {
//            if (row < 0) {
//                // adjust to reflect after end of result set
//                int newRowPosition = this.resultSet.size() + row + 1;
//                if (newRowPosition <= 0) {
//                    beforeFirst();
//                    return false;
//                } else {
//                    return absolute(newRowPosition);
//                }
//            } else {
//                row--; // adjust for index difference
//                this.pos = row;
//                return true;
//            }
//        }

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean previous() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        if ((direction != ResultSet.FETCH_FORWARD) && (direction != ResultSet.FETCH_REVERSE) && (direction != ResultSet.FETCH_UNKNOWN))
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);

        if (!(getType() == ResultSet.TYPE_FORWARD_ONLY && direction == ResultSet.FETCH_FORWARD))
            throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));
        if (rows < 0)
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);

        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return this.resultSet.size();
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Statement getStatement() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return this.statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    /******************************************************************************************************************/
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public int getHoldability() throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (isClosed())
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.RESULT_SET_IS_CLOSED));

        return iface.isInstance(this);
    }
}
