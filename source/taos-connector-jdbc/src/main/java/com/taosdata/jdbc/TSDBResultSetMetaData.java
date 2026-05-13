package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.DataType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class TSDBResultSetMetaData extends WrapperImpl implements ResultSetMetaData {

    private String tableName = "";
    private String database = "";
    private final List<ColumnMetaData> colMetaDataList;

    public TSDBResultSetMetaData(List<ColumnMetaData> metaDataList) {
        this.colMetaDataList = metaDataList;
    }

    public TSDBResultSetMetaData(List<ColumnMetaData> metaDataList, String database, String tableName) {
        this.colMetaDataList = metaDataList;
        this.database = database;
        this.tableName = tableName;
    }

    public int getColumnCount() throws SQLException {
        return colMetaDataList.size();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    public boolean isSearchable(int column) throws SQLException {
        return column == 1;
    }

    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    public int isNullable(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        if (column == 1) {
            return columnNoNulls;
        }
        return columnNullable;
    }

    public boolean isSigned(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        ColumnMetaData meta = this.colMetaDataList.get(column - 1);
        switch (meta.getColType()) {
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
            case TSDBConstants.TSDB_DATA_TYPE_INT:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return colMetaDataList.get(column - 1).getColSize();
    }

    public String getColumnLabel(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return colMetaDataList.get(column - 1).getColName();
    }

    public String getColumnName(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return colMetaDataList.get(column - 1).getColName();
    }

    public String getSchemaName(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    public int getPrecision(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        ColumnMetaData columnMetaData = this.colMetaDataList.get(column - 1);
        switch (columnMetaData.getColType()) {

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return 5;
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return 9;
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                return columnMetaData.getColSize();
            default:
                return 0;
        }
    }

    public int getScale(int column) throws SQLException {
        if (column < 1 && column >= colMetaDataList.size())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);

        return 0;
    }

    public String getTableName(int column) throws SQLException {
        return this.tableName;
    }

    public String getCatalogName(int column) throws SQLException {
        return this.database;
    }

    public int getColumnType(int column) throws SQLException {
        ColumnMetaData meta = this.colMetaDataList.get(column - 1);
        return DataType.convertTaosType2DataType(meta.getColType()).getJdbcTypeValue();
    }

    public String getColumnTypeName(int column) throws SQLException {
        ColumnMetaData meta = this.colMetaDataList.get(column - 1);
        return DataType.convertTaosType2DataType(meta.getColType()).getTypeName();
    }

    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    public String getColumnClassName(int column) throws SQLException {
        int type = this.colMetaDataList.get(column - 1).getColType();
        return DataType.convertTaosType2DataType(type).getClassName();
    }
}
