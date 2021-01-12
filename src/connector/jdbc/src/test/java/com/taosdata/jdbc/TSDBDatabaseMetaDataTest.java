package com.taosdata.jdbc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class TSDBDatabaseMetaDataTest {
    private TSDBDatabaseMetaData metaData;
    private static final String host = "localhost";

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        metaData = (TSDBDatabaseMetaData) DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata", properties).getMetaData();
    }

    @Test
    public void unwrap() throws SQLException {
        TSDBDatabaseMetaData unwrap = metaData.unwrap(TSDBDatabaseMetaData.class);
        Assert.assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(metaData.isWrapperFor(TSDBDatabaseMetaData.class));
    }

    @Test
    public void allProceduresAreCallable() throws SQLException {
        Assert.assertFalse(metaData.allProceduresAreCallable());
    }

    @Test
    public void allTablesAreSelectable() throws SQLException {
        Assert.assertFalse(metaData.allTablesAreSelectable());
    }

    @Test
    public void getURL() throws SQLException {
        Assert.assertEquals("jdbc:TAOS://localhost:6030/?user=root&password=taosdata", metaData.getURL());
    }

    @Test
    public void getUserName() throws SQLException {
        Assert.assertEquals("root", metaData.getUserName());
    }

    @Test
    public void isReadOnly() throws SQLException {
        Assert.assertFalse(metaData.isReadOnly());
    }

    @Test
    public void nullsAreSortedHigh() throws SQLException {
        Assert.assertFalse(metaData.nullsAreSortedHigh());
    }

    @Test
    public void nullsAreSortedLow() throws SQLException {
        Assert.assertTrue(metaData.nullsAreSortedLow());
    }

    @Test
    public void nullsAreSortedAtStart() {
    }

    @Test
    public void nullsAreSortedAtEnd() {
    }

    @Test
    public void getDatabaseProductName() {
    }

    @Test
    public void getDatabaseProductVersion() {
    }

    @Test
    public void getDriverName() {
    }

    @Test
    public void getDriverVersion() {
    }

    @Test
    public void getDriverMajorVersion() {
    }

    @Test
    public void getDriverMinorVersion() {
    }

    @Test
    public void usesLocalFiles() {
    }

    @Test
    public void usesLocalFilePerTable() {
    }

    @Test
    public void supportsMixedCaseIdentifiers() {
    }

    @Test
    public void storesUpperCaseIdentifiers() {
    }

    @Test
    public void storesLowerCaseIdentifiers() {
    }

    @Test
    public void storesMixedCaseIdentifiers() {
    }

    @Test
    public void supportsMixedCaseQuotedIdentifiers() {
    }

    @Test
    public void storesUpperCaseQuotedIdentifiers() {
    }

    @Test
    public void storesLowerCaseQuotedIdentifiers() {
    }

    @Test
    public void storesMixedCaseQuotedIdentifiers() {
    }

    @Test
    public void getIdentifierQuoteString() {
    }

    @Test
    public void getSQLKeywords() {
    }

    @Test
    public void getNumericFunctions() {
    }

    @Test
    public void getStringFunctions() {
    }

    @Test
    public void getSystemFunctions() {
    }

    @Test
    public void getTimeDateFunctions() {
    }

    @Test
    public void getSearchStringEscape() {
    }

    @Test
    public void getExtraNameCharacters() {
    }

    @Test
    public void supportsAlterTableWithAddColumn() {
    }

    @Test
    public void supportsAlterTableWithDropColumn() {
    }

    @Test
    public void supportsColumnAliasing() {
    }

    @Test
    public void nullPlusNonNullIsNull() {
    }

    @Test
    public void supportsConvert() {
    }

    @Test
    public void testSupportsConvert() {
    }

    @Test
    public void supportsTableCorrelationNames() {
    }

    @Test
    public void supportsDifferentTableCorrelationNames() {
    }

    @Test
    public void supportsExpressionsInOrderBy() {
    }

    @Test
    public void supportsOrderByUnrelated() {
    }

    @Test
    public void supportsGroupBy() {
    }

    @Test
    public void supportsGroupByUnrelated() {
    }

    @Test
    public void supportsGroupByBeyondSelect() {
    }

    @Test
    public void supportsLikeEscapeClause() {
    }

    @Test
    public void supportsMultipleResultSets() {
    }

    @Test
    public void supportsMultipleTransactions() {
    }

    @Test
    public void supportsNonNullableColumns() {
    }

    @Test
    public void supportsMinimumSQLGrammar() {
    }

    @Test
    public void supportsCoreSQLGrammar() {
    }

    @Test
    public void supportsExtendedSQLGrammar() {
    }

    @Test
    public void supportsANSI92EntryLevelSQL() {
    }

    @Test
    public void supportsANSI92IntermediateSQL() {
    }

    @Test
    public void supportsANSI92FullSQL() {
    }

    @Test
    public void supportsIntegrityEnhancementFacility() {
    }

    @Test
    public void supportsOuterJoins() {
    }

    @Test
    public void supportsFullOuterJoins() {
    }

    @Test
    public void supportsLimitedOuterJoins() {
    }

    @Test
    public void getSchemaTerm() {
    }

    @Test
    public void getProcedureTerm() {
    }

    @Test
    public void getCatalogTerm() {
    }

    @Test
    public void isCatalogAtStart() {
    }

    @Test
    public void getCatalogSeparator() {
    }

    @Test
    public void supportsSchemasInDataManipulation() {
    }

    @Test
    public void supportsSchemasInProcedureCalls() {
    }

    @Test
    public void supportsSchemasInTableDefinitions() {
    }

    @Test
    public void supportsSchemasInIndexDefinitions() {
    }

    @Test
    public void supportsSchemasInPrivilegeDefinitions() {
    }

    @Test
    public void supportsCatalogsInDataManipulation() {
    }

    @Test
    public void supportsCatalogsInProcedureCalls() {
    }

    @Test
    public void supportsCatalogsInTableDefinitions() {
    }

    @Test
    public void supportsCatalogsInIndexDefinitions() {
    }

    @Test
    public void supportsCatalogsInPrivilegeDefinitions() {
    }

    @Test
    public void supportsPositionedDelete() {
    }

    @Test
    public void supportsPositionedUpdate() {
    }

    @Test
    public void supportsSelectForUpdate() {
    }

    @Test
    public void supportsStoredProcedures() {
    }

    @Test
    public void supportsSubqueriesInComparisons() {
    }

    @Test
    public void supportsSubqueriesInExists() {
    }

    @Test
    public void supportsSubqueriesInIns() {
    }

    @Test
    public void supportsSubqueriesInQuantifieds() {
    }

    @Test
    public void supportsCorrelatedSubqueries() {
    }

    @Test
    public void supportsUnion() {
    }

    @Test
    public void supportsUnionAll() {
    }

    @Test
    public void supportsOpenCursorsAcrossCommit() {
    }

    @Test
    public void supportsOpenCursorsAcrossRollback() {
    }

    @Test
    public void supportsOpenStatementsAcrossCommit() {
    }

    @Test
    public void supportsOpenStatementsAcrossRollback() {
    }

    @Test
    public void getMaxBinaryLiteralLength() {
    }

    @Test
    public void getMaxCharLiteralLength() {
    }

    @Test
    public void getMaxColumnNameLength() {
    }

    @Test
    public void getMaxColumnsInGroupBy() {
    }

    @Test
    public void getMaxColumnsInIndex() {
    }

    @Test
    public void getMaxColumnsInOrderBy() {
    }

    @Test
    public void getMaxColumnsInSelect() {
    }

    @Test
    public void getMaxColumnsInTable() {
    }

    @Test
    public void getMaxConnections() {
    }

    @Test
    public void getMaxCursorNameLength() {
    }

    @Test
    public void getMaxIndexLength() {
    }

    @Test
    public void getMaxSchemaNameLength() {
    }

    @Test
    public void getMaxProcedureNameLength() {
    }

    @Test
    public void getMaxCatalogNameLength() {
    }

    @Test
    public void getMaxRowSize() {
    }

    @Test
    public void doesMaxRowSizeIncludeBlobs() {
    }

    @Test
    public void getMaxStatementLength() {
    }

    @Test
    public void getMaxStatements() {
    }

    @Test
    public void getMaxTableNameLength() {
    }

    @Test
    public void getMaxTablesInSelect() {
    }

    @Test
    public void getMaxUserNameLength() {
    }

    @Test
    public void getDefaultTransactionIsolation() {
    }

    @Test
    public void supportsTransactions() {
    }

    @Test
    public void supportsTransactionIsolationLevel() {
    }

    @Test
    public void supportsDataDefinitionAndDataManipulationTransactions() {
    }

    @Test
    public void supportsDataManipulationTransactionsOnly() {
    }

    @Test
    public void dataDefinitionCausesTransactionCommit() {
    }

    @Test
    public void dataDefinitionIgnoredInTransactions() {
    }

    @Test
    public void getProcedures() {
    }

    @Test
    public void getProcedureColumns() {
    }

    @Test
    public void getTables() {
    }

    @Test
    public void getSchemas() {
    }

    @Test
    public void getCatalogs() {
    }

    @Test
    public void getTableTypes() {
    }

    @Test
    public void getColumns() {
    }

    @Test
    public void getColumnPrivileges() {
    }

    @Test
    public void getTablePrivileges() {
    }

    @Test
    public void getBestRowIdentifier() {
    }

    @Test
    public void getVersionColumns() {
    }

    @Test
    public void getPrimaryKeys() {
    }

    @Test
    public void getImportedKeys() {
    }

    @Test
    public void getExportedKeys() {
    }

    @Test
    public void getCrossReference() {
    }

    @Test
    public void getTypeInfo() {
    }

    @Test
    public void getIndexInfo() {
    }

    @Test
    public void supportsResultSetType() {
    }

    @Test
    public void supportsResultSetConcurrency() {
    }

    @Test
    public void ownUpdatesAreVisible() {
    }

    @Test
    public void ownDeletesAreVisible() {
    }

    @Test
    public void ownInsertsAreVisible() {
    }

    @Test
    public void othersUpdatesAreVisible() {
    }

    @Test
    public void othersDeletesAreVisible() {
    }

    @Test
    public void othersInsertsAreVisible() {
    }

    @Test
    public void updatesAreDetected() {
    }

    @Test
    public void deletesAreDetected() {
    }

    @Test
    public void insertsAreDetected() {
    }

    @Test
    public void supportsBatchUpdates() {
    }

    @Test
    public void getUDTs() {
    }

    @Test
    public void getConnection() {
    }

    @Test
    public void supportsSavepoints() {
    }

    @Test
    public void supportsNamedParameters() {
    }

    @Test
    public void supportsMultipleOpenResults() {
    }

    @Test
    public void supportsGetGeneratedKeys() {
    }

    @Test
    public void getSuperTypes() {
    }

    @Test
    public void getSuperTables() {
    }

    @Test
    public void getAttributes() {
    }

    @Test
    public void supportsResultSetHoldability() {
    }

    @Test
    public void getResultSetHoldability() {
    }

    @Test
    public void getDatabaseMajorVersion() {
    }

    @Test
    public void getDatabaseMinorVersion() {
    }

    @Test
    public void getJDBCMajorVersion() {
    }

    @Test
    public void getJDBCMinorVersion() {
    }

    @Test
    public void getSQLStateType() {
    }

    @Test
    public void locatorsUpdateCopy() {
    }

    @Test
    public void supportsStatementPooling() {
    }

    @Test
    public void getRowIdLifetime() {
    }

    @Test
    public void testGetSchemas() {
    }

    @Test
    public void supportsStoredFunctionsUsingCallSyntax() {
    }

    @Test
    public void autoCommitFailureClosesAllResultSets() {
    }

    @Test
    public void getClientInfoProperties() {
    }

    @Test
    public void getFunctions() {
    }

    @Test
    public void getFunctionColumns() {
    }

    @Test
    public void getPseudoColumns() {
    }

    @Test
    public void generatedKeyAlwaysReturned() {
    }
}