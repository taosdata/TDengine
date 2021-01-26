package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBConstants;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

public class RestfulResultSetMetaData implements ResultSetMetaData {

    private final String database;
    private ArrayList<RestfulResultSet.Field> fields;
    private final RestfulResultSet resultSet;

    public RestfulResultSetMetaData(String database, ArrayList<RestfulResultSet.Field> fields, RestfulResultSet resultSet) {
        this.database = database;
        this.fields = fields;
        this.resultSet = resultSet;
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
        String type = this.fields.get(column - 1).type.toUpperCase();
        switch (type) {
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
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
        String type = this.fields.get(column - 1).type.toUpperCase();
        switch (type) {
            case "FLOAT":
                return 5;
            case "DOUBLE":
                return 9;
            case "BINARY":
            case "NCHAR":
                return this.fields.get(column - 1).length;
            default:
                return 0;
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        String type = this.fields.get(column - 1).type.toUpperCase();
        switch (type) {
            case "FLOAT":
                return 5;
            case "DOUBLE":
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
        String type = this.fields.get(column - 1).type.toUpperCase();
        switch (type) {
            case "BOOL":
                return java.sql.Types.BOOLEAN;
            case "TINYINT":
                return java.sql.Types.TINYINT;
            case "SMALLINT":
                return java.sql.Types.SMALLINT;
            case "INT":
                return java.sql.Types.INTEGER;
            case "BIGINT":
                return java.sql.Types.BIGINT;
            case "FLOAT":
                return java.sql.Types.FLOAT;
            case "DOUBLE":
                return java.sql.Types.DOUBLE;
            case "BINARY":
                return java.sql.Types.BINARY;
            case "TIMESTAMP":
                return java.sql.Types.TIMESTAMP;
            case "NCHAR":
                return java.sql.Types.NCHAR;
        }
        throw new SQLException(TSDBConstants.INVALID_VARIABLES);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        String type = fields.get(column - 1).type;
        return type.toUpperCase();
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
        String type = this.fields.get(column - 1).type;
        String columnClassName = "";
        switch (type) {
            case "BOOL":
                return Boolean.class.getName();
            case "TINYINT":
            case "SMALLINT":
                return Short.class.getName();
            case "INT":
                return Integer.class.getName();
            case "BIGINT":
                return Long.class.getName();
            case "FLOAT":
                return Float.class.getName();
            case "DOUBLE":
                return Double.class.getName();
            case "TIMESTAMP":
                return Timestamp.class.getName();
            case "BINARY":
            case "NCHAR":
                return String.class.getName();
        }
        return columnClassName;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

}
