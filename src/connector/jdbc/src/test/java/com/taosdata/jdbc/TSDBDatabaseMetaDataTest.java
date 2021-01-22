package com.taosdata.jdbc;

import org.junit.*;

import java.sql.*;
import java.util.Properties;

public class TSDBDatabaseMetaDataTest {
    private TSDBDatabaseMetaData metaData;
    private static final String host = "127.0.0.1";
    private Connection connection;

    @BeforeClass
    public void before() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata", properties);
            metaData = connection.getMetaData().unwrap(TSDBDatabaseMetaData.class);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public void after() {
        try {
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
    public void nullsAreSortedAtStart() throws SQLException {
        Assert.assertTrue(metaData.nullsAreSortedAtStart());
    }

    @Test
    public void nullsAreSortedAtEnd() throws SQLException {
        Assert.assertFalse(metaData.nullsAreSortedAtEnd());
    }

    @Test
    public void getDatabaseProductName() throws SQLException {
        Assert.assertEquals("TDengine", metaData.getDatabaseProductName());
    }

    @Test
    public void getDatabaseProductVersion() throws SQLException {
        Assert.assertEquals("2.0.x.x", metaData.getDatabaseProductVersion());
    }

    @Test
    public void getDriverName() throws SQLException {
        Assert.assertEquals("com.taosdata.jdbc.TSDBDriver", metaData.getDriverName());
    }

    @Test
    public void getDriverVersion() throws SQLException {
        Assert.assertEquals("2.0.x", metaData.getDriverVersion());
    }

    @Test
    public void getDriverMajorVersion() {
        Assert.assertEquals(2, metaData.getDriverMajorVersion());
    }

    @Test
    public void getDriverMinorVersion() {
        Assert.assertEquals(0, metaData.getDriverMinorVersion());
    }

    @Test
    public void usesLocalFiles() throws SQLException {
        Assert.assertFalse(metaData.usesLocalFiles());
    }

    @Test
    public void usesLocalFilePerTable() throws SQLException {
        Assert.assertFalse(metaData.usesLocalFilePerTable());
    }

    @Test
    public void supportsMixedCaseIdentifiers() throws SQLException {
        Assert.assertFalse(metaData.supportsMixedCaseIdentifiers());
    }

    @Test
    public void storesUpperCaseIdentifiers() throws SQLException {
        Assert.assertFalse(metaData.storesUpperCaseIdentifiers());
    }

    @Test
    public void storesLowerCaseIdentifiers() throws SQLException {
        Assert.assertTrue(metaData.storesLowerCaseIdentifiers());
    }

    @Test
    public void storesMixedCaseIdentifiers() throws SQLException {
        Assert.assertFalse(metaData.storesMixedCaseIdentifiers());
    }

    @Test
    public void supportsMixedCaseQuotedIdentifiers() throws SQLException {
        Assert.assertFalse(metaData.supportsMixedCaseQuotedIdentifiers());
    }

    @Test
    public void storesUpperCaseQuotedIdentifiers() throws SQLException {
        Assert.assertFalse(metaData.storesUpperCaseQuotedIdentifiers());
    }

    @Test
    public void storesLowerCaseQuotedIdentifiers() throws SQLException {
        Assert.assertFalse(metaData.storesLowerCaseQuotedIdentifiers());
    }

    @Test
    public void storesMixedCaseQuotedIdentifiers() throws SQLException {
        Assert.assertFalse(metaData.storesMixedCaseQuotedIdentifiers());
    }

    @Test
    public void getIdentifierQuoteString() throws SQLException {
        Assert.assertEquals(" ", metaData.getIdentifierQuoteString());
    }

    @Test
    public void getSQLKeywords() throws SQLException {
        Assert.assertEquals(null, metaData.getSQLKeywords());
    }

    @Test
    public void getNumericFunctions() throws SQLException {
        Assert.assertEquals(null, metaData.getNumericFunctions());
    }

    @Test
    public void getStringFunctions() throws SQLException {
        Assert.assertEquals(null, metaData.getStringFunctions());
    }

    @Test
    public void getSystemFunctions() throws SQLException {
        Assert.assertEquals(null, metaData.getSystemFunctions());
    }

    @Test
    public void getTimeDateFunctions() throws SQLException {
        Assert.assertEquals(null, metaData.getTimeDateFunctions());
    }

    @Test
    public void getSearchStringEscape() throws SQLException {
        Assert.assertEquals(null, metaData.getSearchStringEscape());
    }

    @Test
    public void getExtraNameCharacters() throws SQLException {
        Assert.assertEquals(null, metaData.getExtraNameCharacters());
    }

    @Test
    public void supportsAlterTableWithAddColumn() throws SQLException {
        Assert.assertTrue(metaData.supportsAlterTableWithAddColumn());
    }

    @Test
    public void supportsAlterTableWithDropColumn() throws SQLException {
        Assert.assertTrue(metaData.supportsAlterTableWithDropColumn());
    }

    @Test
    public void supportsColumnAliasing() throws SQLException {
        Assert.assertTrue(metaData.supportsColumnAliasing());
    }

    @Test
    public void nullPlusNonNullIsNull() throws SQLException {
        Assert.assertFalse(metaData.nullPlusNonNullIsNull());
    }

    @Test
    public void supportsConvert() throws SQLException {
        Assert.assertFalse(metaData.supportsConvert());
    }

    @Test
    public void testSupportsConvert() throws SQLException {
        Assert.assertFalse(metaData.supportsConvert(1, 1));
    }

    @Test
    public void supportsTableCorrelationNames() throws SQLException {
        Assert.assertFalse(metaData.supportsTableCorrelationNames());
    }

    @Test
    public void supportsDifferentTableCorrelationNames() throws SQLException {
        Assert.assertFalse(metaData.supportsDifferentTableCorrelationNames());
    }

    @Test
    public void supportsExpressionsInOrderBy() throws SQLException {
        Assert.assertFalse(metaData.supportsExpressionsInOrderBy());
    }

    @Test
    public void supportsOrderByUnrelated() throws SQLException {
        Assert.assertFalse(metaData.supportsOrderByUnrelated());
    }

    @Test
    public void supportsGroupBy() throws SQLException {
        Assert.assertTrue(metaData.supportsGroupBy());
    }

    @Test
    public void supportsGroupByUnrelated() throws SQLException {
        Assert.assertFalse(metaData.supportsGroupByUnrelated());
    }

    @Test
    public void supportsGroupByBeyondSelect() throws SQLException {
        Assert.assertFalse(metaData.supportsGroupByBeyondSelect());
    }

    @Test
    public void supportsLikeEscapeClause() throws SQLException {
        Assert.assertFalse(metaData.supportsLikeEscapeClause());
    }

    @Test
    public void supportsMultipleResultSets() throws SQLException {
        Assert.assertFalse(metaData.supportsMultipleResultSets());
    }

    @Test
    public void supportsMultipleTransactions() throws SQLException {
        Assert.assertFalse(metaData.supportsMultipleTransactions());
    }

    @Test
    public void supportsNonNullableColumns() throws SQLException {
        Assert.assertFalse(metaData.supportsNonNullableColumns());
    }

    @Test
    public void supportsMinimumSQLGrammar() throws SQLException {
        Assert.assertFalse(metaData.supportsMinimumSQLGrammar());
    }

    @Test
    public void supportsCoreSQLGrammar() throws SQLException {
        Assert.assertFalse(metaData.supportsCoreSQLGrammar());
    }

    @Test
    public void supportsExtendedSQLGrammar() throws SQLException {
        Assert.assertFalse(metaData.supportsExtendedSQLGrammar());
    }

    @Test
    public void supportsANSI92EntryLevelSQL() throws SQLException {
        Assert.assertFalse(metaData.supportsANSI92EntryLevelSQL());
    }

    @Test
    public void supportsANSI92IntermediateSQL() throws SQLException {
        Assert.assertFalse(metaData.supportsANSI92IntermediateSQL());
    }

    @Test
    public void supportsANSI92FullSQL() throws SQLException {
        Assert.assertFalse(metaData.supportsANSI92FullSQL());
    }

    @Test
    public void supportsIntegrityEnhancementFacility() throws SQLException {
        Assert.assertFalse(metaData.supportsIntegrityEnhancementFacility());
    }

    @Test
    public void supportsOuterJoins() throws SQLException {
        Assert.assertFalse(metaData.supportsOuterJoins());
    }

    @Test
    public void supportsFullOuterJoins() throws SQLException {
        Assert.assertFalse(metaData.supportsFullOuterJoins());
    }

    @Test
    public void supportsLimitedOuterJoins() throws SQLException {
        Assert.assertFalse(metaData.supportsLimitedOuterJoins());
    }

    @Test
    public void getSchemaTerm() throws SQLException {
        Assert.assertNull(metaData.getSchemaTerm());
    }

    @Test
    public void getProcedureTerm() throws SQLException {
        Assert.assertNull(metaData.getProcedureTerm());
    }

    @Test
    public void getCatalogTerm() throws SQLException {
        Assert.assertEquals("database", metaData.getCatalogTerm());
    }

    @Test
    public void isCatalogAtStart() throws SQLException {
        Assert.assertTrue(metaData.isCatalogAtStart());
    }

    @Test
    public void getCatalogSeparator() throws SQLException {
        Assert.assertEquals(".", metaData.getCatalogSeparator());
    }

    @Test
    public void supportsSchemasInDataManipulation() throws SQLException {
        Assert.assertFalse(metaData.supportsSchemasInDataManipulation());
    }

    @Test
    public void supportsSchemasInProcedureCalls() throws SQLException {
        Assert.assertFalse(metaData.supportsSchemasInProcedureCalls());
    }

    @Test
    public void supportsSchemasInTableDefinitions() throws SQLException {
        Assert.assertFalse(metaData.supportsSchemasInTableDefinitions());
    }

    @Test
    public void supportsSchemasInIndexDefinitions() throws SQLException {
        Assert.assertFalse(metaData.supportsSchemasInIndexDefinitions());
    }

    @Test
    public void supportsSchemasInPrivilegeDefinitions() throws SQLException {
        Assert.assertFalse(metaData.supportsSchemasInPrivilegeDefinitions());
    }

    @Test
    public void supportsCatalogsInDataManipulation() throws SQLException {
        Assert.assertTrue(metaData.supportsCatalogsInDataManipulation());
    }

    @Test
    public void supportsCatalogsInProcedureCalls() throws SQLException {
        Assert.assertFalse(metaData.supportsCatalogsInProcedureCalls());
    }

    @Test
    public void supportsCatalogsInTableDefinitions() throws SQLException {
        Assert.assertFalse(metaData.supportsCatalogsInTableDefinitions());
    }

    @Test
    public void supportsCatalogsInIndexDefinitions() throws SQLException {
        Assert.assertFalse(metaData.supportsCatalogsInIndexDefinitions());
    }

    @Test
    public void supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        Assert.assertFalse(metaData.supportsCatalogsInPrivilegeDefinitions());
    }

    @Test
    public void supportsPositionedDelete() throws SQLException {
        Assert.assertFalse(metaData.supportsPositionedDelete());
    }

    @Test
    public void supportsPositionedUpdate() throws SQLException {
        Assert.assertFalse(metaData.supportsPositionedUpdate());
    }

    @Test
    public void supportsSelectForUpdate() throws SQLException {
        Assert.assertFalse(metaData.supportsSelectForUpdate());
    }

    @Test
    public void supportsStoredProcedures() throws SQLException {
        Assert.assertFalse(metaData.supportsStoredProcedures());
    }

    @Test
    public void supportsSubqueriesInComparisons() throws SQLException {
        Assert.assertFalse(metaData.supportsSubqueriesInComparisons());
    }

    @Test
    public void supportsSubqueriesInExists() throws SQLException {
        Assert.assertFalse(metaData.supportsSubqueriesInExists());
    }

    @Test
    public void supportsSubqueriesInIns() throws SQLException {
        Assert.assertFalse(metaData.supportsSubqueriesInIns());
    }

    @Test
    public void supportsSubqueriesInQuantifieds() throws SQLException {
        Assert.assertFalse(metaData.supportsSubqueriesInQuantifieds());
    }

    @Test
    public void supportsCorrelatedSubqueries() throws SQLException {
        Assert.assertFalse(metaData.supportsCorrelatedSubqueries());
    }

    @Test
    public void supportsUnion() throws SQLException {
        Assert.assertFalse(metaData.supportsUnion());
    }

    @Test
    public void supportsUnionAll() throws SQLException {
        Assert.assertFalse(metaData.supportsUnionAll());
    }

    @Test
    public void supportsOpenCursorsAcrossCommit() throws SQLException {
        Assert.assertFalse(metaData.supportsOpenCursorsAcrossCommit());
    }

    @Test
    public void supportsOpenCursorsAcrossRollback() throws SQLException {
        Assert.assertFalse(metaData.supportsOpenCursorsAcrossRollback());
    }

    @Test
    public void supportsOpenStatementsAcrossCommit() throws SQLException {
        Assert.assertFalse(metaData.supportsOpenStatementsAcrossCommit());
    }

    @Test
    public void supportsOpenStatementsAcrossRollback() throws SQLException {
        Assert.assertFalse(metaData.supportsOpenStatementsAcrossRollback());
    }

    @Test
    public void getMaxBinaryLiteralLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxBinaryLiteralLength());
    }

    @Test
    public void getMaxCharLiteralLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxCharLiteralLength());
    }

    @Test
    public void getMaxColumnNameLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxColumnNameLength());
    }

    @Test
    public void getMaxColumnsInGroupBy() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxColumnsInGroupBy());
    }

    @Test
    public void getMaxColumnsInIndex() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxColumnsInIndex());
    }

    @Test
    public void getMaxColumnsInOrderBy() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxColumnsInOrderBy());
    }

    @Test
    public void getMaxColumnsInSelect() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxColumnsInSelect());
    }

    @Test
    public void getMaxColumnsInTable() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxColumnsInTable());
    }

    @Test
    public void getMaxConnections() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxConnections());
    }

    @Test
    public void getMaxCursorNameLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxCursorNameLength());
    }

    @Test
    public void getMaxIndexLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxIndexLength());
    }

    @Test
    public void getMaxSchemaNameLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxSchemaNameLength());
    }

    @Test
    public void getMaxProcedureNameLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxProcedureNameLength());
    }

    @Test
    public void getMaxCatalogNameLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxCatalogNameLength());
    }

    @Test
    public void getMaxRowSize() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxRowSize());
    }

    @Test
    public void doesMaxRowSizeIncludeBlobs() throws SQLException {
        Assert.assertFalse(metaData.doesMaxRowSizeIncludeBlobs());
    }

    @Test
    public void getMaxStatementLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxStatementLength());
    }

    @Test
    public void getMaxStatements() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxStatements());
    }

    @Test
    public void getMaxTableNameLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxTableNameLength());
    }

    @Test
    public void getMaxTablesInSelect() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxTablesInSelect());
    }

    @Test
    public void getMaxUserNameLength() throws SQLException {
        Assert.assertEquals(0, metaData.getMaxUserNameLength());
    }

    @Test
    public void getDefaultTransactionIsolation() throws SQLException {
        Assert.assertEquals(Connection.TRANSACTION_NONE, metaData.getDefaultTransactionIsolation());
    }

    @Test
    public void supportsTransactions() throws SQLException {
        Assert.assertFalse(metaData.supportsTransactions());
    }

    @Test
    public void supportsTransactionIsolationLevel() throws SQLException {
        Assert.assertTrue(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        Assert.assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        Assert.assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        Assert.assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        Assert.assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
    }

    @Test
    public void supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        Assert.assertFalse(metaData.supportsDataDefinitionAndDataManipulationTransactions());
    }

    @Test
    public void supportsDataManipulationTransactionsOnly() throws SQLException {
        Assert.assertFalse(metaData.supportsDataManipulationTransactionsOnly());
    }

    @Test
    public void dataDefinitionCausesTransactionCommit() throws SQLException {
        Assert.assertFalse(metaData.dataDefinitionCausesTransactionCommit());
    }

    @Test
    public void dataDefinitionIgnoredInTransactions() throws SQLException {
        Assert.assertFalse(metaData.dataDefinitionIgnoredInTransactions());
    }

    @Test
    public void getProcedures() throws SQLException {
        Assert.assertNull(metaData.getProcedures("*", "*", "*"));
    }

    @Test
    public void getProcedureColumns() throws SQLException {
        Assert.assertNull(metaData.getProcedureColumns("*", "*", "*", "*"));
    }

    @Test
    public void getTables() throws SQLException {
        System.out.println("****************************************************");
        ResultSet tables = metaData.getTables("log", "", null, null);
        ResultSetMetaData metaData = tables.getMetaData();
        while (tables.next()) {
            System.out.print(metaData.getColumnLabel(1) + ":" + tables.getString(1) + "\t");
            System.out.print(metaData.getColumnLabel(3) + ":" + tables.getString(3) + "\t");
            System.out.print(metaData.getColumnLabel(4) + ":" + tables.getString(4) + "\t");
            System.out.print(metaData.getColumnLabel(5) + ":" + tables.getString(5) + "\n");
        }
        System.out.println();
        Assert.assertNotNull(tables);
    }

    @Test
    public void getSchemas() throws SQLException {
        Assert.assertNotNull(metaData.getSchemas());
    }

    @Test
    public void getCatalogs() throws SQLException {
        System.out.println("****************************************************");

        ResultSet catalogs = metaData.getCatalogs();
        ResultSetMetaData meta = catalogs.getMetaData();
        while (catalogs.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.print(meta.getColumnLabel(i) + ": " + catalogs.getString(i));
            }
            System.out.println();
        }
    }

    @Test
    public void getTableTypes() throws SQLException {
        System.out.println("****************************************************");

        ResultSet tableTypes = metaData.getTableTypes();
        while (tableTypes.next()) {
            System.out.println(tableTypes.getString("TABLE_TYPE"));
        }
        Assert.assertNotNull(metaData.getTableTypes());
    }

    @Test
    public void getColumns() throws SQLException {
        System.out.println("****************************************************");

        ResultSet columns = metaData.getColumns("log", "", "dn", "");
        ResultSetMetaData meta = columns.getMetaData();
        while (columns.next()) {
            System.out.print(meta.getColumnLabel(1) + ": " + columns.getString(1) + "\t");
            System.out.print(meta.getColumnLabel(3) + ": " + columns.getString(3) + "\t");
            System.out.print(meta.getColumnLabel(4) + ": " + columns.getString(4) + "\t");
            System.out.print(meta.getColumnLabel(5) + ": " + columns.getString(5) + "\t");
            System.out.print(meta.getColumnLabel(6) + ": " + columns.getString(6) + "\t");
            System.out.print(meta.getColumnLabel(7) + ": " + columns.getString(7) + "\t");
            System.out.print(meta.getColumnLabel(9) + ": " + columns.getString(9) + "\t");
            System.out.print(meta.getColumnLabel(10) + ": " + columns.getString(10) + "\t");
            System.out.print(meta.getColumnLabel(11) + ": " + columns.getString(11) + "\n");
            System.out.print(meta.getColumnLabel(12) + ": " + columns.getString(12) + "\n");
        }
    }

    @Test
    public void getColumnPrivileges() throws SQLException {
        Assert.assertNotNull(metaData.getColumnPrivileges("", "", "", ""));
    }

    @Test
    public void getTablePrivileges() throws SQLException {
        Assert.assertNotNull(metaData.getTablePrivileges("", "", ""));
    }

    @Test
    public void getBestRowIdentifier() throws SQLException {
        Assert.assertNotNull(metaData.getBestRowIdentifier("", "", "", 0, false));
    }

    @Test
    public void getVersionColumns() throws SQLException {
        Assert.assertNotNull(metaData.getVersionColumns("", "", ""));
    }

    @Test
    public void getPrimaryKeys() throws SQLException {
        System.out.println("****************************************************");

        ResultSet rs = metaData.getPrimaryKeys("log", "", "dn1");
        while (rs.next()) {
            System.out.println("TABLE_NAME: " + rs.getString("TABLE_NAME"));
            System.out.println("COLUMN_NAME: " + rs.getString("COLUMN_NAME"));
            System.out.println("KEY_SEQ: " + rs.getString("KEY_SEQ"));
            System.out.println("PK_NAME: " + rs.getString("PK_NAME"));
        }

        Assert.assertNotNull(rs);
    }

    @Test
    public void getImportedKeys() throws SQLException {
        Assert.assertNotNull(metaData.getImportedKeys("", "", ""));
    }

    @Test
    public void getExportedKeys() throws SQLException {
        Assert.assertNotNull(metaData.getExportedKeys("", "", ""));
    }

    @Test
    public void getCrossReference() throws SQLException {
        Assert.assertNotNull(metaData.getCrossReference("", "", "", "", "", ""));
    }

    @Test
    public void getTypeInfo() throws SQLException {
        Assert.assertNotNull(metaData.getTypeInfo());
    }

    @Test
    public void getIndexInfo() throws SQLException {
        Assert.assertNotNull(metaData.getIndexInfo("", "", "", false, false));
    }

    @Test
    public void supportsResultSetType() throws SQLException {
        Assert.assertFalse(metaData.supportsResultSetType(0));
    }

    @Test
    public void supportsResultSetConcurrency() throws SQLException {
        Assert.assertFalse(metaData.supportsResultSetConcurrency(0, 0));
    }

    @Test
    public void ownUpdatesAreVisible() throws SQLException {
        Assert.assertFalse(metaData.ownUpdatesAreVisible(0));
    }

    @Test
    public void ownDeletesAreVisible() throws SQLException {
        Assert.assertFalse(metaData.ownDeletesAreVisible(0));
    }

    @Test
    public void ownInsertsAreVisible() throws SQLException {
        Assert.assertFalse(metaData.ownInsertsAreVisible(0));
    }

    @Test
    public void othersUpdatesAreVisible() throws SQLException {
        Assert.assertFalse(metaData.othersUpdatesAreVisible(0));
    }

    @Test
    public void othersDeletesAreVisible() throws SQLException {
        Assert.assertFalse(metaData.othersDeletesAreVisible(0));
    }

    @Test
    public void othersInsertsAreVisible() throws SQLException {
        Assert.assertFalse(metaData.othersInsertsAreVisible(0));
    }

    @Test
    public void updatesAreDetected() throws SQLException {
        Assert.assertFalse(metaData.updatesAreDetected(0));
    }

    @Test
    public void deletesAreDetected() throws SQLException {
        Assert.assertFalse(metaData.deletesAreDetected(0));
    }

    @Test
    public void insertsAreDetected() throws SQLException {
        Assert.assertFalse(metaData.insertsAreDetected(0));
    }

    @Test
    public void supportsBatchUpdates() throws SQLException {
        Assert.assertFalse(metaData.supportsBatchUpdates());
    }

    @Test
    public void getUDTs() throws SQLException {
        Assert.assertNotNull(metaData.getUDTs("", "", "", null));
    }

    @Test
    public void getConnection() throws SQLException {
        Assert.assertNotNull(metaData.getConnection());
    }

    @Test
    public void supportsSavepoints() throws SQLException {
        Assert.assertFalse(metaData.supportsSavepoints());
    }

    @Test
    public void supportsNamedParameters() throws SQLException {
        Assert.assertFalse(metaData.supportsNamedParameters());
    }

    @Test
    public void supportsMultipleOpenResults() throws SQLException {
        Assert.assertFalse(metaData.supportsMultipleOpenResults());
    }

    @Test
    public void supportsGetGeneratedKeys() throws SQLException {
        Assert.assertFalse(metaData.supportsGetGeneratedKeys());
    }

    @Test
    public void getSuperTypes() throws SQLException {
        Assert.assertNotNull(metaData.getSuperTypes("", "", ""));
    }

    @Test
    public void getSuperTables() throws SQLException {
        System.out.println("****************************************************");

        ResultSet rs = metaData.getSuperTables("log", "", "dn1");
        while (rs.next()) {
            System.out.println("TABLE_NAME: " + rs.getString("TABLE_NAME"));
            System.out.println("SUPERTABLE_NAME: " + rs.getString("SUPERTABLE_NAME"));
        }
        Assert.assertNotNull(rs);
    }

    @Test
    public void getAttributes() throws SQLException {
        Assert.assertNotNull(metaData.getAttributes("", "", "", ""));
    }

    @Test
    public void supportsResultSetHoldability() throws SQLException {
        Assert.assertTrue(metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        Assert.assertFalse(metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test
    public void getResultSetHoldability() throws SQLException {
        Assert.assertEquals(1, metaData.getResultSetHoldability());
    }

    @Test
    public void getDatabaseMajorVersion() throws SQLException {
        Assert.assertEquals(2, metaData.getDatabaseMajorVersion());
    }

    @Test
    public void getDatabaseMinorVersion() throws SQLException {
        Assert.assertEquals(0, metaData.getDatabaseMinorVersion());
    }

    @Test
    public void getJDBCMajorVersion() throws SQLException {
        Assert.assertEquals(2, metaData.getJDBCMajorVersion());
    }

    @Test
    public void getJDBCMinorVersion() throws SQLException {
        Assert.assertEquals(0, metaData.getJDBCMinorVersion());
    }

    @Test
    public void getSQLStateType() throws SQLException {
        Assert.assertEquals(0, metaData.getSQLStateType());
    }

    @Test
    public void locatorsUpdateCopy() throws SQLException {
        Assert.assertFalse(metaData.locatorsUpdateCopy());
    }

    @Test
    public void supportsStatementPooling() throws SQLException {
        Assert.assertFalse(metaData.supportsStatementPooling());
    }

    @Test
    public void getRowIdLifetime() throws SQLException {
        Assert.assertNull(metaData.getRowIdLifetime());
    }

    @Test
    public void supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        Assert.assertFalse(metaData.supportsStoredFunctionsUsingCallSyntax());
    }

    @Test
    public void autoCommitFailureClosesAllResultSets() throws SQLException {
        Assert.assertFalse(metaData.autoCommitFailureClosesAllResultSets());
    }

    @Test
    public void getClientInfoProperties() throws SQLException {
        Assert.assertNotNull(metaData.getClientInfoProperties());
    }

    @Test
    public void getFunctions() throws SQLException {
        Assert.assertNotNull(metaData.getFunctions("", "", ""));
    }

    @Test
    public void getFunctionColumns() throws SQLException {
        Assert.assertNotNull(metaData.getFunctionColumns("", "", "", ""));
    }

    @Test
    public void getPseudoColumns() throws SQLException {
        Assert.assertNotNull(metaData.getPseudoColumns("", "", "", ""));
    }

    @Test
    public void generatedKeyAlwaysReturned() throws SQLException {
        Assert.assertFalse(metaData.generatedKeyAlwaysReturned());
    }
}