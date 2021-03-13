package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.WrapperImpl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

public class RestfulResultSetMetaData extends WrapperImpl implements ResultSetMetaData {

    private final String database;
    private ArrayList<RestfulResultSet.Field> fields;
    private final RestfulResultSet resultSet;

    public RestfulResultSetMetaData(String database, ArrayList<RestfulResultSet.Field> fields, RestfulResultSet resultSet) {
        this.database = database;
        this.fields = fields;
        this.resultSet = resultSet;
    }

    public ArrayList<RestfulResultSet.Field> getFields() {
        return fields;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return fields.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        if (column == 1)
            return ResultSetMetaData.columnNoNulls;
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int type = this.fields.get(column - 1).type;
        switch (type) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.DOUBLE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return this.fields.get(column - 1).length;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return fields.get(column - 1).name;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return fields.get(column - 1).name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        int type = this.fields.get(column - 1).type;
        switch (type) {
            case Types.FLOAT:
                return 5;
            case Types.DOUBLE:
                return 9;
            case Types.BINARY:
            case Types.NCHAR:
                return this.fields.get(column - 1).length;
            default:
                return 0;
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        int type = this.fields.get(column - 1).type;
        switch (type) {
            case Types.FLOAT:
                return 5;
            case Types.DOUBLE:
                return 9;
            default:
                return 0;
        }
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return this.database;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return this.fields.get(column - 1).type;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int taos_type = fields.get(column - 1).taos_type;
        return TSDBConstants.taosType2JdbcTypeName(taos_type);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = this.fields.get(column - 1).type;
        String columnClassName = "";
        switch (type) {
            case Types.BOOLEAN:
                return Boolean.class.getName();
            case Types.TINYINT:
            case Types.SMALLINT:
                return Short.class.getName();
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.FLOAT:
                return Float.class.getName();
            case Types.DOUBLE:
                return Double.class.getName();
            case Types.TIMESTAMP:
                return Timestamp.class.getName();
            case Types.BINARY:
            case Types.NCHAR:
                return String.class.getName();
        }
        return columnClassName;
    }

}
