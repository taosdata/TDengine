/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TSDBDatabaseMetaData extends AbstractDatabaseMetaData {

    private String url;
    private String userName;
    private Connection conn;

    public TSDBDatabaseMetaData(String url, String userName) {
        this.url = url;
        this.userName = userName;
    }

    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public String getURL() throws SQLException {
        return this.url;
    }

    public String getUserName() throws SQLException {
        return this.userName;
    }

    public String getDriverName() throws SQLException {
        return TSDBDriver.class.getName();
    }

    /**
     * @Param catalog : database名称，"" 表示不属于任何database的table，null表示不使用database来缩小范围
     * @Param schemaPattern : schema名称，""表示
     * @Param tableNamePattern : 表名满足tableNamePattern的表, null表示返回所有表
     * @Param types : 表类型，null表示返回所有类型
     */
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (conn == null || conn.isClosed()) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }

        try (Statement stmt = conn.createStatement()) {
            if (catalog == null || catalog.isEmpty())
                return null;

            ResultSet databases = stmt.executeQuery("show databases");
            String dbname = null;
            while (databases.next()) {
                dbname = databases.getString("name");
                if (dbname.equalsIgnoreCase(catalog))
                    break;
            }
            databases.close();
            if (dbname == null)
                return null;

            stmt.execute("use " + dbname);
            DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
            List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
            ColumnMetaData col1 = new ColumnMetaData();
            col1.setColIndex(1);
            col1.setColName("TABLE_CAT");
            col1.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col1);
            ColumnMetaData col2 = new ColumnMetaData();
            col2.setColIndex(2);
            col2.setColName("TABLE_SCHEM");
            col2.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col2);
            ColumnMetaData col3 = new ColumnMetaData();
            col3.setColIndex(3);
            col3.setColName("TABLE_NAME");
            col3.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col3);
            ColumnMetaData col4 = new ColumnMetaData();
            col4.setColIndex(4);
            col4.setColName("TABLE_TYPE");
            col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col4);
            ColumnMetaData col5 = new ColumnMetaData();
            col5.setColIndex(5);
            col5.setColName("REMARKS");
            col5.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col5);
            ColumnMetaData col6 = new ColumnMetaData();
            col6.setColIndex(6);
            col6.setColName("TYPE_CAT");
            col6.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col6);
            ColumnMetaData col7 = new ColumnMetaData();
            col7.setColIndex(7);
            col7.setColName("TYPE_SCHEM");
            col7.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col7);
            ColumnMetaData col8 = new ColumnMetaData();
            col8.setColIndex(8);
            col8.setColName("TYPE_NAME");
            col8.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col8);
            ColumnMetaData col9 = new ColumnMetaData();
            col9.setColIndex(9);
            col9.setColName("SELF_REFERENCING_COL_NAME");
            col9.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col9);
            ColumnMetaData col10 = new ColumnMetaData();
            col10.setColIndex(10);
            col10.setColName("REF_GENERATION");
            col10.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col10);
            resultSet.setColumnMetaDataList(columnMetaDataList);

            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            ResultSet tables = stmt.executeQuery("show tables");
            while (tables.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                rowData.setString(0, dbname);
                rowData.setString(2, tables.getString("table_name"));
                rowData.setString(3, "TABLE");
                rowData.setString(4, "");
                rowDataList.add(rowData);
            }

            ResultSet stables = stmt.executeQuery("show stables");
            while (stables.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                rowData.setString(0, dbname);
                rowData.setString(2, stables.getString("name"));
                rowData.setString(3, "TABLE");
                rowData.setString(4, "STABLE");
                rowDataList.add(rowData);
            }
            resultSet.setRowDataList(rowDataList);
            return resultSet;
        }
    }

    public ResultSet getCatalogs() throws SQLException {
        if (conn == null || conn.isClosed())
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));

        try (Statement stmt = conn.createStatement()) {
            DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
            // set up ColumnMetaDataList
            List<ColumnMetaData> columnMetaDataList = new ArrayList<>(24);
            // TABLE_CAT
            ColumnMetaData col1 = new ColumnMetaData();
            col1.setColIndex(1);
            col1.setColName("TABLE_CAT");
            col1.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col1);
            resultSet.setColumnMetaDataList(columnMetaDataList);

            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("show databases");
            while (rs.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(1);
                rowData.setString(0, rs.getString("name"));
                rowDataList.add(rowData);
            }
            resultSet.setRowDataList(rowDataList);
            return resultSet;
        }
    }

    public ResultSet getTableTypes() throws SQLException {
        if (conn == null || conn.isClosed())
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set up ColumnMetaDataList
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        ColumnMetaData colMetaData = new ColumnMetaData();
        colMetaData.setColIndex(0);
        colMetaData.setColName("TABLE_TYPE");
        colMetaData.setColSize(10);
        colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        columnMetaDataList.add(colMetaData);
        resultSet.setColumnMetaDataList(columnMetaDataList);

        // set up rowDataList
        List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
        TSDBResultSetRowData rowData = new TSDBResultSetRowData(1);
        rowData.setString(0, "TABLE");
        rowDataList.add(rowData);
        rowData = new TSDBResultSetRowData(1);
        rowData.setString(0, "STABLE");
        rowDataList.add(rowData);
        resultSet.setRowDataList(rowDataList);

        return resultSet;
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        if (conn == null || conn.isClosed())
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));

        try (Statement stmt = conn.createStatement()) {
            if (catalog == null || catalog.isEmpty())
                return null;

            ResultSet databases = stmt.executeQuery("show databases");
            String dbname = null;
            while (databases.next()) {
                dbname = databases.getString("name");
                if (dbname.equalsIgnoreCase(catalog))
                    break;
            }
            databases.close();
            if (dbname == null)
                return null;

            stmt.execute("use " + dbname);
            DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
            // set up ColumnMetaDataList
            List<ColumnMetaData> columnMetaDataList = new ArrayList<>(24);
            // TABLE_CAT
            ColumnMetaData col1 = new ColumnMetaData();
            col1.setColIndex(1);
            col1.setColName("TABLE_CAT");
            col1.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col1);
            // TABLE_SCHEM
            ColumnMetaData col2 = new ColumnMetaData();
            col2.setColIndex(2);
            col2.setColName("TABLE_SCHEM");
            col2.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col2);
            // TABLE_NAME
            ColumnMetaData col3 = new ColumnMetaData();
            col3.setColIndex(3);
            col3.setColName("TABLE_NAME");
            col3.setColSize(193);
            col3.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col3);
            // COLUMN_NAME
            ColumnMetaData col4 = new ColumnMetaData();
            col4.setColIndex(4);
            col4.setColName("COLUMN_NAME");
            col4.setColSize(65);
            col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col4);
            // DATA_TYPE
            ColumnMetaData col5 = new ColumnMetaData();
            col5.setColIndex(5);
            col5.setColName("DATA_TYPE");
            col5.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(col5);
            // TYPE_NAME
            ColumnMetaData col6 = new ColumnMetaData();
            col6.setColIndex(6);
            col6.setColName("TYPE_NAME");
            col6.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col6);
            // COLUMN_SIZE
            ColumnMetaData col7 = new ColumnMetaData();
            col7.setColIndex(7);
            col7.setColName("COLUMN_SIZE");
            col7.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(col7);
            // BUFFER_LENGTH ,not used
            columnMetaDataList.add(null);
            // DECIMAL_DIGITS
            ColumnMetaData col9 = new ColumnMetaData();
            col9.setColIndex(9);
            col9.setColName("DECIMAL_DIGITS");
            col9.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(col9);
            // add NUM_PREC_RADIX
            ColumnMetaData col10 = new ColumnMetaData();
            col10.setColIndex(10);
            col10.setColName("NUM_PREC_RADIX");
            col10.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(col10);
            // NULLABLE
            ColumnMetaData col11 = new ColumnMetaData();
            col11.setColIndex(11);
            col11.setColName("NULLABLE");
            col11.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(col11);
            // REMARKS
            ColumnMetaData col12 = new ColumnMetaData();
            col12.setColIndex(12);
            col12.setColName("REMARKS");
            col12.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col12);
            resultSet.setColumnMetaDataList(columnMetaDataList);


            // set up rowDataList
            ResultSet rs = stmt.executeQuery("describe " + dbname + "." + tableNamePattern);
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            int index = 0;
            while (rs.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(24);
                // set TABLE_CAT
                rowData.setString(0, dbname);
                // set TABLE_NAME
                rowData.setString(2, tableNamePattern);
                // set COLUMN_NAME
                rowData.setString(3, rs.getString("Field"));
                // set DATA_TYPE
                String typeName = rs.getString("Type");
                rowData.setInt(4, getDataType(typeName));
                // set TYPE_NAME
                rowData.setString(5, typeName);
                // set COLUMN_SIZE
                int length = rs.getInt("Length");
                rowData.setInt(6, getColumnSize(typeName, length));
                // set DECIMAL_DIGITS
                rowData.setInt(8, getDecimalDigits(typeName));
                // set NUM_PREC_RADIX
                rowData.setInt(9, 10);
                // set NULLABLE
                rowData.setInt(10, getNullable(index, typeName));
                // set REMARKS
                rowData.setString(11, rs.getString("Note"));
                rowDataList.add(rowData);
                index++;
            }
            resultSet.setRowDataList(rowDataList);
            return resultSet;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if (conn == null || conn.isClosed())
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));

        try (Statement stmt = conn.createStatement()) {
            if (catalog == null || catalog.isEmpty())
                return null;

            ResultSet databases = stmt.executeQuery("show databases");
            String dbname = null;
            while (databases.next()) {
                dbname = databases.getString("name");
                if (dbname.equalsIgnoreCase(catalog))
                    break;
            }
            databases.close();
            if (dbname == null)
                return null;

            stmt.execute("use " + dbname);
            DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
            // set up ColumnMetaDataList
            List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
            // TABLE_CAT
            ColumnMetaData col1 = new ColumnMetaData();
            col1.setColIndex(0);
            col1.setColName("TABLE_CAT");
            col1.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col1);
            // TABLE_SCHEM
            ColumnMetaData col2 = new ColumnMetaData();
            col2.setColIndex(1);
            col2.setColName("TABLE_SCHEM");
            col2.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col2);
            // TABLE_NAME
            ColumnMetaData col3 = new ColumnMetaData();
            col3.setColIndex(2);
            col3.setColName("TABLE_NAME");
            col3.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col3);
            // COLUMN_NAME
            ColumnMetaData col4 = new ColumnMetaData();
            col4.setColIndex(3);
            col4.setColName("COLUMN_NAME");
            col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col4);
            // KEY_SEQ
            ColumnMetaData col5 = new ColumnMetaData();
            col5.setColIndex(4);
            col5.setColName("KEY_SEQ");
            col5.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
            columnMetaDataList.add(col5);
            // PK_NAME
            ColumnMetaData col6 = new ColumnMetaData();
            col6.setColIndex(5);
            col6.setColName("PK_NAME");
            col6.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col6);
            resultSet.setColumnMetaDataList(columnMetaDataList);

            // set rowData
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("describe " + dbname + "." + table);
            rs.next();
            TSDBResultSetRowData rowData = new TSDBResultSetRowData(6);
            rowData.setString(0, null);
            rowData.setString(1, null);
            rowData.setString(2, table);
            String pkName = rs.getString(1);
            rowData.setString(3, pkName);
            rowData.setInt(4, 1);
            rowData.setString(5, pkName);
            rowDataList.add(rowData);
            resultSet.setRowDataList(rowDataList);
            return resultSet;
        }
    }


    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws
            SQLException {
        if (conn == null || conn.isClosed())
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));

        try (Statement stmt = conn.createStatement()) {
            if (catalog == null || catalog.isEmpty())
                return null;

            ResultSet databases = stmt.executeQuery("show databases");
            String dbname = null;
            while (databases.next()) {
                dbname = databases.getString("name");
                if (dbname.equalsIgnoreCase(catalog))
                    break;
            }
            databases.close();
            if (dbname == null)
                return null;

            stmt.execute("use " + dbname);
            DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
            // set up ColumnMetaDataList
            List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
            // TABLE_CAT
            ColumnMetaData col1 = new ColumnMetaData();
            col1.setColIndex(0);
            col1.setColName("TABLE_CAT");
            col1.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col1);
            // TABLE_SCHEM
            ColumnMetaData col2 = new ColumnMetaData();
            col2.setColIndex(1);
            col2.setColName("TABLE_SCHEM");
            col2.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col2);
            // TABLE_NAME
            ColumnMetaData col3 = new ColumnMetaData();
            col3.setColIndex(2);
            col3.setColName("TABLE_NAME");
            col3.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col3);
            // SUPERTABLE_NAME
            ColumnMetaData col4 = new ColumnMetaData();
            col4.setColIndex(3);
            col4.setColName("SUPERTABLE_NAME");
            col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col4);
            resultSet.setColumnMetaDataList(columnMetaDataList);

            ResultSet rs = stmt.executeQuery("show tables like '" + tableNamePattern + "'");
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            while (rs.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(4);
                rowData.setString(2, rs.getString(1));
                rowData.setString(3, rs.getString(4));
                rowDataList.add(rowData);
            }
            resultSet.setRowDataList(rowDataList);
            return resultSet;
        }
    }

}