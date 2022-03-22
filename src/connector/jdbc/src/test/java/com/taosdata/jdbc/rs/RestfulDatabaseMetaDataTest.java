package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class RestfulDatabaseMetaDataTest {

    private static final String host = "127.0.0.1";
    private static final String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
    private static Connection connection;
    private static RestfulDatabaseMetaData metaData;
    private static final String dbName = "test";

    @Test
    public void unwrap() throws SQLException {
        RestfulDatabaseMetaData unwrap = metaData.unwrap(RestfulDatabaseMetaData.class);
        Assert.assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(metaData.isWrapperFor(RestfulDatabaseMetaData.class));
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
        Assert.assertEquals(url, metaData.getURL());
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
        Assert.assertEquals("com.taosdata.jdbc.rs.RestfulDriver", metaData.getDriverName());
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
        ResultSet rs = metaData.getTables("log", "", null, null);
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertNotNull(rs);
        rs.next();
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertEquals("log", rs.getString(1));
            Assert.assertEquals("log", rs.getString("TABLE_CAT"));
            // TABLE_SCHEM
            Assert.assertEquals("TABLE_SCHEM", meta.getColumnLabel(2));
            Assert.assertEquals(null, rs.getString(2));
            Assert.assertEquals(null, rs.getString("TABLE_SCHEM"));
            // TABLE_NAME
            Assert.assertEquals("TABLE_NAME", meta.getColumnLabel(3));
            Assert.assertNotNull(rs.getString(3));
            Assert.assertNotNull(rs.getString("TABLE_NAME"));
            // TABLE_TYPE
            Assert.assertEquals("TABLE_TYPE", meta.getColumnLabel(4));
            Assert.assertEquals("TABLE", rs.getString(4));
            Assert.assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            // REMARKS
            Assert.assertEquals("REMARKS", meta.getColumnLabel(5));
            Assert.assertEquals("", rs.getString(5));
            Assert.assertEquals("", rs.getString("REMARKS"));
        }
    }

    @Test
    public void getSchemas() throws SQLException {
        Assert.assertNotNull(metaData.getSchemas());
    }

    @Test
    public void getCatalogs() throws SQLException {
        ResultSet rs = metaData.getCatalogs();
        ResultSetMetaData meta = rs.getMetaData();
        rs.next();
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertNotNull(rs.getString(1));
            Assert.assertNotNull(rs.getString("TABLE_CAT"));
        }
    }

    @Test
    public void getTableTypes() throws SQLException {
        ResultSet tableTypes = metaData.getTableTypes();
        tableTypes.next();
        // tableTypes: table
        {
            Assert.assertEquals("TABLE", tableTypes.getString(1));
            Assert.assertEquals("TABLE", tableTypes.getString("TABLE_TYPE"));
        }
        tableTypes.next();
        // tableTypes: stable
        {
            Assert.assertEquals("STABLE", tableTypes.getString(1));
            Assert.assertEquals("STABLE", tableTypes.getString("TABLE_TYPE"));
        }
    }

    @Test
    public void getColumns() throws SQLException {
        // when
        ResultSet columns = metaData.getColumns("log", "", "dn", "");
        // then
        ResultSetMetaData meta = columns.getMetaData();
        columns.next();
        // column: 1
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertEquals("log", columns.getString(1));
            Assert.assertEquals("log", columns.getString("TABLE_CAT"));
            // TABLE_NAME
            Assert.assertEquals("TABLE_NAME", meta.getColumnLabel(3));
            Assert.assertEquals("dn", columns.getString(3));
            Assert.assertEquals("dn", columns.getString("TABLE_NAME"));
            // COLUMN_NAME
            Assert.assertEquals("COLUMN_NAME", meta.getColumnLabel(4));
            Assert.assertEquals("ts", columns.getString(4));
            Assert.assertEquals("ts", columns.getString("COLUMN_NAME"));
            // DATA_TYPE
            Assert.assertEquals("DATA_TYPE", meta.getColumnLabel(5));
            Assert.assertEquals(Types.TIMESTAMP, columns.getInt(5));
            Assert.assertEquals(Types.TIMESTAMP, columns.getInt("DATA_TYPE"));
            // TYPE_NAME
            Assert.assertEquals("TYPE_NAME", meta.getColumnLabel(6));
            Assert.assertEquals("TIMESTAMP", columns.getString(6));
            Assert.assertEquals("TIMESTAMP", columns.getString("TYPE_NAME"));
            // COLUMN_SIZE
            Assert.assertEquals("COLUMN_SIZE", meta.getColumnLabel(7));
            Assert.assertEquals(26, columns.getInt(7));
            Assert.assertEquals(26, columns.getInt("COLUMN_SIZE"));
            // DECIMAL_DIGITS
            Assert.assertEquals("DECIMAL_DIGITS", meta.getColumnLabel(9));
            Assert.assertEquals(0, columns.getInt(9));
            Assert.assertEquals(0, columns.getInt("DECIMAL_DIGITS"));
            Assert.assertEquals(null, columns.getString(9));
            Assert.assertEquals(null, columns.getString("DECIMAL_DIGITS"));
            // NUM_PREC_RADIX
            Assert.assertEquals("NUM_PREC_RADIX", meta.getColumnLabel(10));
            Assert.assertEquals(10, columns.getInt(10));
            Assert.assertEquals(10, columns.getInt("NUM_PREC_RADIX"));
            // NULLABLE
            Assert.assertEquals("NULLABLE", meta.getColumnLabel(11));
            Assert.assertEquals(DatabaseMetaData.columnNoNulls, columns.getInt(11));
            Assert.assertEquals(DatabaseMetaData.columnNoNulls, columns.getInt("NULLABLE"));
            // REMARKS
            Assert.assertEquals("REMARKS", meta.getColumnLabel(12));
            Assert.assertEquals(null, columns.getString(12));
            Assert.assertEquals(null, columns.getString("REMARKS"));
        }
        columns.next();
        // column: 2
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertEquals("log", columns.getString(1));
            Assert.assertEquals("log", columns.getString("TABLE_CAT"));
            // TABLE_NAME
            Assert.assertEquals("TABLE_NAME", meta.getColumnLabel(3));
            Assert.assertEquals("dn", columns.getString(3));
            Assert.assertEquals("dn", columns.getString("TABLE_NAME"));
            // COLUMN_NAME
            Assert.assertEquals("COLUMN_NAME", meta.getColumnLabel(4));
            Assert.assertEquals("cpu_taosd", columns.getString(4));
            Assert.assertEquals("cpu_taosd", columns.getString("COLUMN_NAME"));
            // DATA_TYPE
            Assert.assertEquals("DATA_TYPE", meta.getColumnLabel(5));
            Assert.assertEquals(Types.FLOAT, columns.getInt(5));
            Assert.assertEquals(Types.FLOAT, columns.getInt("DATA_TYPE"));
            // TYPE_NAME
            Assert.assertEquals("TYPE_NAME", meta.getColumnLabel(6));
            Assert.assertEquals("FLOAT", columns.getString(6));
            Assert.assertEquals("FLOAT", columns.getString("TYPE_NAME"));
            // COLUMN_SIZE
            Assert.assertEquals("COLUMN_SIZE", meta.getColumnLabel(7));
            Assert.assertEquals(12, columns.getInt(7));
            Assert.assertEquals(12, columns.getInt("COLUMN_SIZE"));
            // DECIMAL_DIGITS
            Assert.assertEquals("DECIMAL_DIGITS", meta.getColumnLabel(9));
            Assert.assertEquals(0, columns.getInt(9));
            Assert.assertEquals(0, columns.getInt("DECIMAL_DIGITS"));
            Assert.assertEquals(null, columns.getString(9));
            Assert.assertEquals(null, columns.getString("DECIMAL_DIGITS"));
            // NUM_PREC_RADIX
            Assert.assertEquals("NUM_PREC_RADIX", meta.getColumnLabel(10));
            Assert.assertEquals(10, columns.getInt(10));
            Assert.assertEquals(10, columns.getInt("NUM_PREC_RADIX"));
            // NULLABLE
            Assert.assertEquals("NULLABLE", meta.getColumnLabel(11));
            Assert.assertEquals(DatabaseMetaData.columnNullable, columns.getInt(11));
            Assert.assertEquals(DatabaseMetaData.columnNullable, columns.getInt("NULLABLE"));
            // REMARKS
            Assert.assertEquals("REMARKS", meta.getColumnLabel(12));
            Assert.assertEquals(null, columns.getString(12));
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
        ResultSet rs = metaData.getPrimaryKeys("log", "", "dn1");
        ResultSetMetaData meta = rs.getMetaData();
        rs.next();
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertEquals("log", rs.getString(1));
            Assert.assertEquals("log", rs.getString("TABLE_CAT"));
            // TABLE_SCHEM
            Assert.assertEquals("TABLE_SCHEM", meta.getColumnLabel(2));
            Assert.assertEquals(null, rs.getString(2));
            Assert.assertEquals(null, rs.getString("TABLE_SCHEM"));
            // TABLE_NAME
            Assert.assertEquals("TABLE_NAME", meta.getColumnLabel(3));
            Assert.assertEquals("dn1", rs.getString(3));
            Assert.assertEquals("dn1", rs.getString("TABLE_NAME"));
            // COLUMN_NAME
            Assert.assertEquals("COLUMN_NAME", meta.getColumnLabel(4));
            Assert.assertEquals("ts", rs.getString(4));
            Assert.assertEquals("ts", rs.getString("COLUMN_NAME"));
            // KEY_SEQ
            Assert.assertEquals("KEY_SEQ", meta.getColumnLabel(5));
            Assert.assertEquals(1, rs.getShort(5));
            Assert.assertEquals(1, rs.getShort("KEY_SEQ"));
            // DATA_TYPE
            Assert.assertEquals("PK_NAME", meta.getColumnLabel(6));
            Assert.assertEquals("ts", rs.getString(6));
            Assert.assertEquals("ts", rs.getString("PK_NAME"));
        }
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
        ResultSet rs = metaData.getSuperTables("log", "", "dn1");
        ResultSetMetaData meta = rs.getMetaData();
        rs.next();
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertEquals("log", rs.getString(1));
            Assert.assertEquals("log", rs.getString("TABLE_CAT"));
            // TABLE_CAT
            Assert.assertEquals("TABLE_SCHEM", meta.getColumnLabel(2));
            Assert.assertEquals(null, rs.getString(2));
            Assert.assertEquals(null, rs.getString("TABLE_SCHEM"));
            // TABLE_CAT
            Assert.assertEquals("TABLE_NAME", meta.getColumnLabel(3));
            Assert.assertEquals("dn1", rs.getString(3));
            Assert.assertEquals("dn1", rs.getString("TABLE_NAME"));
            // TABLE_CAT
            Assert.assertEquals("SUPERTABLE_NAME", meta.getColumnLabel(4));
            Assert.assertEquals("dn", rs.getString(4));
            Assert.assertEquals("dn", rs.getString("SUPERTABLE_NAME"));
        }
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

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connection = DriverManager.getConnection(url, properties);
        Statement stmt = connection.createStatement();
        stmt.execute("drop database if exists " + dbName);
        stmt.execute("create database if not exists " + dbName + " precision 'us'");
        stmt.execute("use " + dbName);
        stmt.execute("create table `dn` (ts TIMESTAMP,cpu_taosd FLOAT,cpu_system FLOAT,cpu_cores INT,mem_taosd FLOAT,mem_system FLOAT,mem_total INT,disk_used FLOAT,disk_total INT,band_speed FLOAT,io_read FLOAT,io_write FLOAT,req_http INT,req_select INT,req_insert INT) TAGS (dnodeid INT,fqdn BINARY(128))");
        stmt.execute("insert into dn1 using dn tags(1,'a') (ts) values(now)");

        metaData = connection.getMetaData().unwrap(RestfulDatabaseMetaData.class);
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null)
            connection.close();
    }

}