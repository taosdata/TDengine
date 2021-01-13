package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class RestfulDatabaseMetaData extends AbstractDatabaseMetaData {

    private final String url;
    private final String userName;
    private final Connection connection;

    public RestfulDatabaseMetaData(String url, String userName, Connection connection) {
        this.url = url;
        this.userName = userName;
        this.connection = connection;
    }

    @Override
    public String getURL() throws SQLException {
        return this.url;
    }

    @Override
    public String getUserName() throws SQLException {
        return this.userName;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        Statement stmt = null;
        if (null != connection && !connection.isClosed()) {
            stmt = connection.createStatement();
            if (catalog == null || catalog.length() < 1) {
                catalog = connection.getCatalog();
            }
            stmt.executeUpdate("use " + catalog);
            ResultSet resultSet0 = stmt.executeQuery("show tables");
            GetTablesResultSet getTablesResultSet = new GetTablesResultSet(resultSet0, catalog, schemaPattern, tableNamePattern, types);
            return getTablesResultSet;
        } else {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            Statement stmt = connection.createStatement();
            ResultSet resultSet0 = stmt.executeQuery("show databases");
            CatalogResultSet resultSet = new CatalogResultSet(resultSet0);
            return resultSet;
        } else {
            return new EmptyResultSet();
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Statement stmt = null;
        if (null != connection && !connection.isClosed()) {
            stmt = connection.createStatement();
            if (catalog == null || catalog.length() < 1) {
                catalog = connection.getCatalog();
            }
            stmt.execute("use " + catalog);

            DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
            // set up ColumnMetaDataList
            List<ColumnMetaData> columnMetaDataList = new ArrayList<>(24);
            columnMetaDataList.add(null);
            columnMetaDataList.add(null);
            // add TABLE_NAME
            ColumnMetaData colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(3);
            colMetaData.setColName("TABLE_NAME");
            colMetaData.setColSize(193);
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_BINARY);
            columnMetaDataList.add(colMetaData);
            // add COLUMN_NAME
            colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(4);
            colMetaData.setColName("COLUMN_NAME");
            colMetaData.setColSize(65);
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_BINARY);
            columnMetaDataList.add(colMetaData);
            // add DATA_TYPE
            colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(5);
            colMetaData.setColName("DATA_TYPE");
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(colMetaData);
            // add TYPE_NAME
            colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(6);
            colMetaData.setColName("TYPE_NAME");
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_BINARY);
            columnMetaDataList.add(colMetaData);
            // add COLUMN_SIZE
            colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(7);
            colMetaData.setColName("COLUMN_SIZE");
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(colMetaData);
            // add BUFFER_LENGTH ,not used
            columnMetaDataList.add(null);
            // add DECIMAL_DIGITS
            colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(9);
            colMetaData.setColName("DECIMAL_DIGITS");
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(colMetaData);
            // add NUM_PREC_RADIX
            colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(10);
            colMetaData.setColName("NUM_PREC_RADIX");
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(colMetaData);
            // add NULLABLE
            colMetaData = new ColumnMetaData();
            colMetaData.setColIndex(11);
            colMetaData.setColName("NULLABLE");
            colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(colMetaData);

            resultSet.setColumnMetaDataList(columnMetaDataList);

            // set up rowDataList
            ResultSet resultSet0 = stmt.executeQuery("describe " + tableNamePattern);
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            int index = 0;
            while (resultSet0.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(24);
                // set TABLE_NAME
                rowData.setString(2, tableNamePattern);
                // set COLUMN_NAME
                rowData.setString(3, resultSet0.getString(1));
                // set DATA_TYPE
                String typeName = resultSet0.getString(2);
                rowData.setInt(4, getDataType(typeName));
                // set TYPE_NAME
                rowData.setString(5, typeName);
                // set COLUMN_SIZE
                int length = resultSet0.getInt(3);
                rowData.setInt(6, getColumnSize(typeName, length));
                // set DECIMAL_DIGITS
                rowData.setInt(8, getDecimalDigits(typeName));
                // set NUM_PREC_RADIX
                rowData.setInt(9, 10);
                // set NULLABLE
                rowData.setInt(10, getNullable(index, typeName));
                rowDataList.add(rowData);
                index++;
            }
            resultSet.setRowDataList(rowDataList);

            return resultSet;
        } else {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return false;
    }


    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
