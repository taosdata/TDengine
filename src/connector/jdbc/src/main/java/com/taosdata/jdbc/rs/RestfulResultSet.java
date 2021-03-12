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
    // data
    private ArrayList<ArrayList<Object>> resultSet;
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
        // column metadata
        JSONArray columnMeta = resultJson.getJSONArray("column_meta");
        columnNames = new ArrayList<>();
        columns = new ArrayList<>();
        for (int colIndex = 0; colIndex < columnMeta.size(); colIndex++) {
            JSONArray col = columnMeta.getJSONArray(colIndex);
            String col_name = col.getString(0);
            int col_type = TSDBConstants.taosType2JdbcType(col.getInteger(1));
            int col_length = col.getInteger(2);
            columnNames.add(col_name);
            columns.add(new Field(col_name, col_type, col_length, ""));
        }
        this.metaData = new RestfulResultSetMetaData(this.database, columns, this);

        // row data
        JSONArray data = resultJson.getJSONArray("data");
        resultSet = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
            ArrayList row = new ArrayList();
            JSONArray jsonRow = data.getJSONArray(rowIndex);
            for (int colIndex = 0; colIndex < jsonRow.size(); colIndex++) {
                row.add(parseColumnData(jsonRow, colIndex, columns.get(colIndex).type));
            }
            resultSet.add(row);
        }

        /*
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
         */
    }

    private Object parseColumnData(JSONArray row, int colIndex, int sqlType) {
        switch (sqlType) {
            case Types.NULL:
                return null;
            case Types.BOOLEAN:
                return row.getBoolean(colIndex);
            case Types.TINYINT:
            case Types.SMALLINT:
                return row.getShort(colIndex);
            case Types.INTEGER:
                return row.getInteger(colIndex);
            case Types.BIGINT:
                return row.getBigInteger(colIndex);
            case Types.FLOAT:
                return row.getFloat(colIndex);
            case Types.DOUBLE:
                return row.getDouble(colIndex);
            case Types.TIMESTAMP:
                return new Timestamp(row.getDate(colIndex).getTime());
            case Types.BINARY:
            case Types.NCHAR:
            default:
                return row.getString(colIndex);
        }
    }

//    /**
//     * 由多个resultSet的JSON构造结果集
//     *
//     * @param resultJson: 包含data信息的结果集，有sql返回的结果集
//     * @param fieldJson:  包含多个（最多2个）meta信息的结果集，有describe xxx
//     **/
//    public RestfulResultSet(String database, Statement statement, JSONObject resultJson, List<JSONObject> fieldJson) throws SQLException {
//        this(database, statement, resultJson);
//        ArrayList<Field> newColumns = new ArrayList<>();
//
//        for (Field column : columns) {
//            Field field = findField(column.name, fieldJson);
//            if (field != null) {
//                newColumns.add(field);
//            } else {
//                newColumns.add(column);
//            }
//        }
//        this.columns = newColumns;
//        this.metaData = new RestfulResultSetMetaData(this.database, this.columns, this);
//    }

//    public Field findField(String columnName, List<JSONObject> fieldJsonList) {
//        for (JSONObject fieldJSON : fieldJsonList) {
//            JSONArray fieldDataJson = fieldJSON.getJSONArray("data");
//            for (int i = 0; i < fieldDataJson.size(); i++) {
//                JSONArray field = fieldDataJson.getJSONArray(i);
//                if (columnName.equalsIgnoreCase(field.getString(0))) {
//                    return new Field(field.getString(0), field.getString(1), field.getInteger(2), field.getString(3));
//                }
//            }
//        }
//        return null;
//    }

    public class Field {
        String name;
        int type;
        int length;
        String note;

        public Field(String name, int type, int length, String note) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.note = note;
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

        if (columnIndex > resultSet.get(pos).size()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "Column Index out of range, " + columnIndex + " > " + resultSet.get(pos).size());
        }

        columnIndex = getTrueColumnIndex(columnIndex);
        return resultSet.get(pos).get(columnIndex).toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        columnIndex = getTrueColumnIndex(columnIndex);
        int result = getInt(columnIndex);
        return result == 0 ? false : true;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        columnIndex = getTrueColumnIndex(columnIndex);
        return Short.parseShort(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        columnIndex = getTrueColumnIndex(columnIndex);
        return Integer.parseInt(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        columnIndex = getTrueColumnIndex(columnIndex);
        return Long.parseLong(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        columnIndex = getTrueColumnIndex(columnIndex);
        return Float.parseFloat(resultSet.get(pos).get(columnIndex).toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

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
        String strDate = resultSet.get(pos).get(columnIndex).toString();
//        strDate = strDate.substring(1, strDate.length() - 1);
        return Timestamp.valueOf(strDate);
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
