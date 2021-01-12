package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class DatabaseMetaDataTest {
    static Connection connection = null;
    static PreparedStatement statement = null;
    static String dbName = "test";
    static String tName = "t0";
    static String host = "localhost";

    @BeforeClass
    public static void createConnection() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

        String sql = "drop database if exists " + dbName;
        statement = connection.prepareStatement(sql);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
    }

    @Test
    public void testMetaDataTest() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getTables(dbName, "t*", "t*", new String[]{"t"});
        while (resultSet.next()) {
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                System.out.printf("%d: %s\n", i, resultSet.getString(i));
            }
        }
        resultSet.close();
        databaseMetaData.isWrapperFor(null);
        databaseMetaData.allProceduresAreCallable();
        databaseMetaData.allTablesAreSelectable();
        databaseMetaData.getURL();
        databaseMetaData.getUserName();
        databaseMetaData.isReadOnly();
        databaseMetaData.nullsAreSortedHigh();
        databaseMetaData.nullsAreSortedLow();
        databaseMetaData.nullsAreSortedAtStart();
        databaseMetaData.nullsAreSortedAtEnd();
        databaseMetaData.getDatabaseProductName();
        databaseMetaData.getDatabaseProductVersion();
        databaseMetaData.getDriverName();
        databaseMetaData.getDriverVersion();
        databaseMetaData.getDriverMajorVersion();
        databaseMetaData.getDriverMinorVersion();
        databaseMetaData.usesLocalFiles();
        databaseMetaData.usesLocalFilePerTable();
        databaseMetaData.supportsMixedCaseIdentifiers();
        databaseMetaData.storesUpperCaseIdentifiers();
        databaseMetaData.storesLowerCaseIdentifiers();
        databaseMetaData.storesMixedCaseIdentifiers();
        databaseMetaData.supportsMixedCaseQuotedIdentifiers();
        databaseMetaData.storesUpperCaseQuotedIdentifiers();
        databaseMetaData.storesLowerCaseQuotedIdentifiers();
        databaseMetaData.storesMixedCaseQuotedIdentifiers();
        databaseMetaData.getIdentifierQuoteString();
        databaseMetaData.getSQLKeywords();
        databaseMetaData.getNumericFunctions();
        databaseMetaData.getStringFunctions();
        databaseMetaData.getSystemFunctions();
        databaseMetaData.getTimeDateFunctions();
        databaseMetaData.getSearchStringEscape();
        databaseMetaData.getExtraNameCharacters();
        databaseMetaData.supportsAlterTableWithAddColumn();
        databaseMetaData.supportsAlterTableWithDropColumn();
        databaseMetaData.supportsColumnAliasing();
        databaseMetaData.nullPlusNonNullIsNull();
        databaseMetaData.supportsConvert();
        databaseMetaData.supportsConvert(0, 0);
        databaseMetaData.supportsTableCorrelationNames();
        databaseMetaData.supportsDifferentTableCorrelationNames();
        databaseMetaData.supportsExpressionsInOrderBy();
        databaseMetaData.supportsOrderByUnrelated();
        databaseMetaData.supportsGroupBy();
        databaseMetaData.supportsGroupByUnrelated();
        databaseMetaData.supportsGroupByBeyondSelect();
        databaseMetaData.supportsLikeEscapeClause();
        databaseMetaData.supportsMultipleResultSets();
        databaseMetaData.supportsMultipleTransactions();
        databaseMetaData.supportsNonNullableColumns();
        databaseMetaData.supportsMinimumSQLGrammar();
        databaseMetaData.supportsCoreSQLGrammar();
        databaseMetaData.supportsExtendedSQLGrammar();
        databaseMetaData.supportsANSI92EntryLevelSQL();
        databaseMetaData.supportsANSI92IntermediateSQL();
        databaseMetaData.supportsANSI92FullSQL();
        databaseMetaData.supportsIntegrityEnhancementFacility();
        databaseMetaData.supportsOuterJoins();
        databaseMetaData.supportsFullOuterJoins();
        databaseMetaData.supportsLimitedOuterJoins();
        databaseMetaData.getSchemaTerm();
        databaseMetaData.getProcedureTerm();
        databaseMetaData.getCatalogTerm();
        databaseMetaData.isCatalogAtStart();
        databaseMetaData.getCatalogSeparator();
        databaseMetaData.supportsSchemasInDataManipulation();
        databaseMetaData.supportsSchemasInProcedureCalls();
        databaseMetaData.supportsSchemasInTableDefinitions();
        databaseMetaData.supportsSchemasInIndexDefinitions();
        databaseMetaData.supportsSchemasInPrivilegeDefinitions();
        databaseMetaData.supportsCatalogsInDataManipulation();
        databaseMetaData.supportsCatalogsInProcedureCalls();
        databaseMetaData.supportsCatalogsInTableDefinitions();
        databaseMetaData.supportsCatalogsInIndexDefinitions();
        databaseMetaData.supportsCatalogsInPrivilegeDefinitions();
        databaseMetaData.supportsPositionedDelete();
        databaseMetaData.supportsPositionedUpdate();
        databaseMetaData.supportsSelectForUpdate();
        databaseMetaData.supportsStoredProcedures();
        databaseMetaData.supportsSubqueriesInComparisons();
        databaseMetaData.supportsSubqueriesInExists();
        databaseMetaData.supportsSubqueriesInIns();
        databaseMetaData.supportsSubqueriesInQuantifieds();
        databaseMetaData.supportsCorrelatedSubqueries();
        databaseMetaData.supportsUnion();
        databaseMetaData.supportsUnionAll();
        databaseMetaData.supportsOpenCursorsAcrossCommit();
        databaseMetaData.supportsOpenCursorsAcrossRollback();
        databaseMetaData.supportsOpenStatementsAcrossCommit();
        databaseMetaData.supportsOpenStatementsAcrossRollback();
        databaseMetaData.getMaxBinaryLiteralLength();
        databaseMetaData.getMaxCharLiteralLength();
        databaseMetaData.getMaxColumnNameLength();
        databaseMetaData.getMaxColumnsInGroupBy();
        databaseMetaData.getMaxColumnsInIndex();
        databaseMetaData.getMaxColumnsInOrderBy();
        databaseMetaData.getMaxColumnsInSelect();
        databaseMetaData.getMaxColumnsInTable();
        databaseMetaData.getMaxConnections();
        databaseMetaData.getMaxCursorNameLength();
        databaseMetaData.getMaxIndexLength();
        databaseMetaData.getMaxSchemaNameLength();
        databaseMetaData.getMaxProcedureNameLength();
        databaseMetaData.getMaxCatalogNameLength();
        databaseMetaData.getMaxRowSize();
        databaseMetaData.doesMaxRowSizeIncludeBlobs();
        databaseMetaData.getMaxStatementLength();
        databaseMetaData.getMaxStatements();
        databaseMetaData.getMaxTableNameLength();
        databaseMetaData.getMaxTablesInSelect();
        databaseMetaData.getMaxUserNameLength();
        databaseMetaData.getDefaultTransactionIsolation();
        databaseMetaData.supportsTransactions();
        databaseMetaData.supportsTransactionIsolationLevel(0);
        databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions();
        databaseMetaData.supportsDataManipulationTransactionsOnly();
        databaseMetaData.dataDefinitionCausesTransactionCommit();
        databaseMetaData.dataDefinitionIgnoredInTransactions();
        try {
            databaseMetaData.getProcedures("", "", "");
        } catch (Exception e) {
        }
        try {
            databaseMetaData.getProcedureColumns("", "", "", "");
        } catch (Exception e) {
        }
        try {
            databaseMetaData.getTables("", "", "", new String[]{""});
        } catch (Exception e) {
        }
        databaseMetaData.getSchemas();
        databaseMetaData.getCatalogs();
//        databaseMetaData.getTableTypes();

        databaseMetaData.getColumns(dbName, "", tName, "");
        databaseMetaData.getColumnPrivileges("", "", "", "");
        databaseMetaData.getTablePrivileges("", "", "");
        databaseMetaData.getBestRowIdentifier("", "", "", 0, false);
        databaseMetaData.getVersionColumns("", "", "");
        databaseMetaData.getPrimaryKeys("", "", "");
        databaseMetaData.getImportedKeys("", "", "");
        databaseMetaData.getExportedKeys("", "", "");
        databaseMetaData.getCrossReference("", "", "", "", "", "");
        databaseMetaData.getTypeInfo();
        databaseMetaData.getIndexInfo("", "", "", false, false);
        databaseMetaData.supportsResultSetType(0);
        databaseMetaData.supportsResultSetConcurrency(0, 0);
        databaseMetaData.ownUpdatesAreVisible(0);
        databaseMetaData.ownDeletesAreVisible(0);
        databaseMetaData.ownInsertsAreVisible(0);
        databaseMetaData.othersUpdatesAreVisible(0);
        databaseMetaData.othersDeletesAreVisible(0);
        databaseMetaData.othersInsertsAreVisible(0);
        databaseMetaData.updatesAreDetected(0);
        databaseMetaData.deletesAreDetected(0);
        databaseMetaData.insertsAreDetected(0);
        databaseMetaData.supportsBatchUpdates();
        databaseMetaData.getUDTs("", "", "", new int[]{0});
        databaseMetaData.getConnection();
        databaseMetaData.supportsSavepoints();
        databaseMetaData.supportsNamedParameters();
        databaseMetaData.supportsMultipleOpenResults();
        databaseMetaData.supportsGetGeneratedKeys();
        databaseMetaData.getSuperTypes("", "", "");
        databaseMetaData.getSuperTables("", "", "");
        databaseMetaData.getAttributes("", "", "", "");
        databaseMetaData.supportsResultSetHoldability(0);
        databaseMetaData.getResultSetHoldability();
        databaseMetaData.getDatabaseMajorVersion();
        databaseMetaData.getDatabaseMinorVersion();
        databaseMetaData.getJDBCMajorVersion();
        databaseMetaData.getJDBCMinorVersion();
        databaseMetaData.getSQLStateType();
        databaseMetaData.locatorsUpdateCopy();
        databaseMetaData.supportsStatementPooling();
        databaseMetaData.getRowIdLifetime();
        databaseMetaData.getSchemas("", "");
        databaseMetaData.supportsStoredFunctionsUsingCallSyntax();
        databaseMetaData.autoCommitFailureClosesAllResultSets();
        databaseMetaData.getClientInfoProperties();
        databaseMetaData.getFunctions("", "", "");
        databaseMetaData.getFunctionColumns("", "", "", "");
        databaseMetaData.getPseudoColumns("", "", "", "");
        databaseMetaData.generatedKeyAlwaysReturned();
    }


    @AfterClass
    public static void close() throws Exception {
        statement.executeUpdate("drop database " + dbName);
        statement.close();
        connection.close();
        Thread.sleep(10);

    }
}
