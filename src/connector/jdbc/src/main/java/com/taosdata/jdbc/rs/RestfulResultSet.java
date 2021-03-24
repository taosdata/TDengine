package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;

import java.sql.*;
import java.util.ArrayList;

public class RestfulResultSet extends AbstractResultSet implements ResultSet {
    private volatile boolean isClosed;
    private int pos = -1;

    private final String database;
    private final Statement statement;
//    private final JSONObject resultJson;
    // data
    private final ArrayList<ArrayList<Object>> resultSet;
    // meta
    private ArrayList<String> columnNames;
    private ArrayList<Field> columns;
    private RestfulResultSetMetaData metaData;

    /**
     * 由一个result的Json构造结果集，对应执行show databases, show tables等这些语句，返回结果集，但无法获取结果集对应的meta，统一当成String处理
     *
     * @param resultJson: 包含data信息的结果集，有sql返回的结果集
     ***/
    public RestfulResultSet(String database, Statement statement, JSONObject resultJson) throws SQLException {
        this.database = database;
        this.statement = statement;
//        this.resultJson = resultJson;

        // column metadata
        JSONArray columnMeta = resultJson.getJSONArray("column_meta");
        columnNames = new ArrayList<>();
        columns = new ArrayList<>();
        for (int colIndex = 0; colIndex < columnMeta.size(); colIndex++) {
            JSONArray col = columnMeta.getJSONArray(colIndex);
            String col_name = col.getString(0);
            int taos_type = col.getInteger(1);
            int col_type = TSDBConstants.taosType2JdbcType(taos_type);
            int col_length = col.getInteger(2);
            columnNames.add(col_name);
            columns.add(new Field(col_name, col_type, col_length, "", taos_type));
        }
        this.metaData = new RestfulResultSetMetaData(this.database, columns, this);

        // row data
        JSONArray data = resultJson.getJSONArray("data");
        resultSet = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
            ArrayList row = new ArrayList();
            JSONArray jsonRow = data.getJSONArray(rowIndex);
            for (int colIndex = 0; colIndex < jsonRow.size(); colIndex++) {
                row.add(parseColumnData(jsonRow, colIndex, columns.get(colIndex).taos_type));
            }
            resultSet.add(row);
        }
    }

    private Object parseColumnData(JSONArray row, int colIndex, int taosType) {
        switch (taosType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return row.getBoolean(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return row.getByte(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return row.getShort(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return row.getInteger(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return row.getBigInteger(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return row.getFloat(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return row.getDouble(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                return new Timestamp(row.getDate(colIndex).getTime());
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            default:
                return row.getString(colIndex);
        }
    }

    public class Field {
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
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
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
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return resultSet.isEmpty();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        Object value = resultSet.get(pos).get(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        Object value = resultSet.get(pos).get(columnIndex);
        return value != null && (Boolean) value;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        Object value = resultSet.get(pos).get(columnIndex);
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
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        Object value = resultSet.get(pos).get(columnIndex);
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
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        Object value = resultSet.get(pos).get(columnIndex);
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
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        Object value = resultSet.get(pos).get(columnIndex);
        if (value == null)
            return 0;

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
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        return Float.parseFloat(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (columnIndex > resultSet.get(pos).size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());

        columnIndex = getTrueColumnIndex(columnIndex);
        return Double.parseDouble(resultSet.get(pos).get(columnIndex).toString());
    }

    private int getTrueColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE
                    , "Column Index out of range, " + columnIndex + " < 1");
        }

        int numOfCols = resultSet.get(pos).size();
        if (columnIndex > numOfCols) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE
                    , "Column Index out of range, " + columnIndex + " > " + numOfCols);
        }

        return columnIndex - 1;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        columnIndex = getTrueColumnIndex(columnIndex);
        Object value = resultSet.get(pos).get(columnIndex);
        return value == null ? null : (Timestamp) value;
    }

    /*************************************************************************************************************/
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.metaData;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        int fieldColumnIndex = getTrueColumnIndex(columnIndex);
        Field field = this.columns.get(fieldColumnIndex);
        switch (field.taos_type) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return this.getBoolean(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (int) this.getByte(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return this.getShort(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return this.getInt(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return this.getLong(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return this.getFloat(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return this.getDouble(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                return this.getTimestamp(columnIndex);
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            default:
                return this.getString(columnIndex);
        }
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
    public boolean isClosed() throws SQLException {
        return isClosed;
    }


}
