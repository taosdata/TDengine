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

public abstract class AbstractDatabaseMetaData implements DatabaseMetaData {

    private final static String PRODUCT_NAME = "TDengine";
    private final static String PRODUCT_VESION = "2.0.x.x";
    private final static String DRIVER_NAME = "taos-jdbcdriver";
    private final static String DRIVER_VERSION = "2.0.x";
    private final static int DRIVER_MAJAR_VERSION = 2;
    private final static int DRIVER_MINOR_VERSION = 0;

    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    public abstract String getURL() throws SQLException;

    public abstract String getUserName() throws SQLException;

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
        return PRODUCT_NAME;
    }

    public String getDatabaseProductVersion() throws SQLException {
        return PRODUCT_VESION;
    }

    public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    public String getDriverVersion() throws SQLException {
        return DRIVER_VERSION;
    }

    public int getDriverMajorVersion() {
        return DRIVER_MAJAR_VERSION;
    }

    public int getDriverMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
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
        return false;
    }

    public boolean supportsConvert() throws SQLException {
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
        return false;
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
        return 0;
    }

    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
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

    public abstract ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException;

    public ResultSet getSchemas() throws SQLException {
        return getEmptyResultSet();
    }

    public abstract ResultSet getCatalogs() throws SQLException;

    public ResultSet getTableTypes() throws SQLException {
        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();

        // set up ColumnMetaDataList
        List<ColumnMetaData> columnMetaDataList = new ArrayList<ColumnMetaData>(1);
        ColumnMetaData colMetaData = new ColumnMetaData();
        colMetaData.setColIndex(0);
        colMetaData.setColName("TABLE_TYPE");
        colMetaData.setColSize(10);
        colMetaData.setColType(TSDBConstants.TSDB_DATA_TYPE_BINARY);
        columnMetaDataList.add(colMetaData);

        // set up rowDataList
        List<TSDBResultSetRowData> rowDataList = new ArrayList<TSDBResultSetRowData>(2);
        TSDBResultSetRowData rowData = new TSDBResultSetRowData();
        rowData.setString(0, "TABLE");
        rowDataList.add(rowData);
        rowData = new TSDBResultSetRowData();
        rowData.setString(0, "STABLE");
        rowDataList.add(rowData);

        resultSet.setColumnMetaDataList(columnMetaDataList);
        resultSet.setRowDataList(rowDataList);
        return resultSet;
    }

    public abstract ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException;

    protected int getNullable(int index, String typeName) {
        if (index == 0 && "TIMESTAMP".equals(typeName))
            return DatabaseMetaData.columnNoNulls;
        return DatabaseMetaData.columnNullable;
    }

    protected int getColumnSize(String typeName, int length) {
        switch (typeName) {
            case "TIMESTAMP":
                return 23;

            default:
                return 0;
        }
    }

    protected int getDecimalDigits(String typeName) {
        switch (typeName) {
            case "FLOAT":
                return 5;
            case "DOUBLE":
                return 9;
            default:
                return 0;
        }
    }

    protected int getDataType(String typeName) {
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
        return getEmptyResultSet();
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
        return null;
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

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    public int getJDBCMajorVersion() throws SQLException {
        return 0;
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