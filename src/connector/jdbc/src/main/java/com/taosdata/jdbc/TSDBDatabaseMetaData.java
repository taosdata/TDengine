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

public class TSDBDatabaseMetaData implements java.sql.DatabaseMetaData {

    private String url;
    private String userName;
    private Connection conn;

    public TSDBDatabaseMetaData(String url, String userName) {
        this.url = url;
        this.userName = userName;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString());
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    public String getURL() throws SQLException {
        return this.url;
    }

    public String getUserName() throws SQLException {
        return this.userName;
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return !nullsAreSortedHigh();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return !nullsAreSortedAtStart();
    }

    public String getDatabaseProductName() throws SQLException {
        return "TDengine";
    }

    public String getDatabaseProductVersion() throws SQLException {
        return "2.0.x.x";
    }

    public String getDriverName() throws SQLException {
        return TSDBDriver.class.getName();
    }

    public String getDriverVersion() throws SQLException {
        return "2.0.x";
    }

    public int getDriverMajorVersion() {
        return 2;
    }

    public int getDriverMinorVersion() {
        return 0;
    }

    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }


    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        //像database、table这些对象的标识符，在存储时是否采用大小写混合的模式
        return false;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        //像database、table这些对象的标识符，在存储时是否采用大小写混合、并带引号的模式
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public String getIdentifierQuoteString() throws SQLException {
        return " ";
    }

    public String getSQLKeywords() throws SQLException {
        return null;
    }

    public String getNumericFunctions() throws SQLException {
        return null;
    }

    public String getStringFunctions() throws SQLException {
        return null;
    }

    public String getSystemFunctions() throws SQLException {
        return null;
    }

    public String getTimeDateFunctions() throws SQLException {
        return null;
    }

    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        // null + non-null != null
        return false;
    }

    public boolean supportsConvert() throws SQLException {
        // 是否支持转换函数convert
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    public String getSchemaTerm() throws SQLException {
        return null;
    }

    public String getProcedureTerm() throws SQLException {
        return null;
    }

    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    public boolean supportsUnion() throws SQLException {
        return false;
    }

    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    public int getMaxConnections() throws SQLException {
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    public int getMaxStatements() throws SQLException {
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        if (level == Connection.TRANSACTION_NONE)
            return true;
        return false;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return null;
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return null;
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
                rowData.setString(2, tables.getString("table_name"));
                rowData.setString(3, "TABLE");
                rowData.setString(4, "");
                rowDataList.add(rowData);
            }

            ResultSet stables = stmt.executeQuery("show stables");
            while (stables.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                rowData.setString(2, stables.getString("name"));
                rowData.setString(3, "TABLE");
                rowData.setString(4, "STABLE");
                rowDataList.add(rowData);
            }
            resultSet.setRowDataList(rowDataList);
            return resultSet;
        }
    }

    public ResultSet getSchemas() throws SQLException {
        return getEmptyResultSet();
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

    private int getNullable(int index, String typeName) {
        if (index == 0 && "TIMESTAMP".equals(typeName))
            return DatabaseMetaData.columnNoNulls;
        return DatabaseMetaData.columnNullable;
    }

    private int getColumnSize(String typeName, int length) {
        switch (typeName) {
            case "TIMESTAMP":
                return 23;

            default:
                return 0;
        }
    }

    private int getDecimalDigits(String typeName) {
        switch (typeName) {
            case "FLOAT":
                return 5;
            case "DOUBLE":
                return 9;
            default:
                return 0;
        }
    }

    private int getDataType(String typeName) {
        switch (typeName) {
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "INT":
                return Types.INTEGER;
            case "BIGINT":
                return Types.BIGINT;
            case "FLOAT":
                return Types.FLOAT;
            case "DOUBLE":
                return Types.DOUBLE;
            case "BINARY":
                return Types.BINARY;
            case "SMALLINT":
                return Types.SMALLINT;
            case "TINYINT":
                return Types.TINYINT;
            case "BOOL":
                return Types.BOOLEAN;
            case "NCHAR":
                return Types.NCHAR;
            default:
                return Types.NULL;
        }
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
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

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getTypeInfo() throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return getEmptyResultSet();
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return getEmptyResultSet();
    }

    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws
            SQLException {
        return getEmptyResultSet();
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
            col3.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col3);
            // SUPERTABLE_NAME
            ColumnMetaData col4 = new ColumnMetaData();
            col4.setColIndex(4);
            col4.setColName("SUPERTABLE_NAME");
            col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
            columnMetaDataList.add(col4);
            resultSet.setColumnMetaDataList(columnMetaDataList);

            ResultSet rs = stmt.executeQuery("show talbes like '" + tableNamePattern + "'");
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

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        if (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT)
            return true;
        return false;
    }

    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return 2;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    public int getJDBCMajorVersion() throws SQLException {
        return 2;
    }

    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    public int getSQLStateType() throws SQLException {
        return 0;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    private ResultSet getEmptyResultSet() {
        return new EmptyResultSet();
    }
}