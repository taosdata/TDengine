package com.taosdata.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDatabaseMetaData extends WrapperImpl implements DatabaseMetaData {

    private final static String PRODUCT_NAME = "TDengine";
    private final static String PRODUCT_VESION = "2.0.x.x";
    private final static String DRIVER_VERSION = "2.0.x";
    private final static int DRIVER_MAJAR_VERSION = 2;
    private final static int DRIVER_MINOR_VERSION = 0;

    private String precision = "ms";
    private String database;

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

    public abstract String getDriverName() throws SQLException;

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

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    public abstract ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException;

    private List<ColumnMetaData> buildGetTablesColumnMetaDataList() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableCatalogMeta(1));          // 1. TABLE_CAT
        columnMetaDataList.add(buildTableSchemaMeta(2));           // 2. TABLE_SCHEM
        columnMetaDataList.add(buildTableNameMeta(3));             // 3. TABLE_NAME
        columnMetaDataList.add(buildTableTypeMeta(4));             // 4. TABLE_TYPE
        columnMetaDataList.add(buildRemarksMeta(5));                // 5. remarks
        columnMetaDataList.add(buildTypeCatMeta(6));                // 6. TYPE_CAT
        columnMetaDataList.add(buildTypeSchemaMeta(7));             // 7. TYPE_SCHEM
        columnMetaDataList.add(buildTypeNameMeta(8));               // 8. TYPE_NAME
        columnMetaDataList.add(buildSelfReferencingColName(9));     // 9. SELF_REFERENCING_COL_NAME
        columnMetaDataList.add(buildRefGenerationMeta(10));         // 10. REF_GENERATION
        return columnMetaDataList;
    }

    private ColumnMetaData buildTypeCatMeta(int colIndex) {
        ColumnMetaData col6 = new ColumnMetaData();
        col6.setColIndex(colIndex);
        col6.setColName("TYPE_CAT");
        col6.setColType(Types.NCHAR);
        return col6;
    }

    private ColumnMetaData buildTypeSchemaMeta(int colIndex) {
        ColumnMetaData col7 = new ColumnMetaData();
        col7.setColIndex(colIndex);
        col7.setColName("TYPE_SCHEM");
        col7.setColType(Types.NCHAR);
        return col7;
    }

    private ColumnMetaData buildTypeNameMeta(int colIndex) {
        ColumnMetaData col8 = new ColumnMetaData();
        col8.setColIndex(colIndex);
        col8.setColName("TYPE_NAME");
        col8.setColType(Types.NCHAR);
        return col8;
    }

    private ColumnMetaData buildSelfReferencingColName(int colIndex) {
        ColumnMetaData col9 = new ColumnMetaData();
        col9.setColIndex(colIndex);
        col9.setColName("SELF_REFERENCING_COL_NAME");
        col9.setColType(Types.NCHAR);
        return col9;
    }

    private ColumnMetaData buildRefGenerationMeta(int colIndex) {
        ColumnMetaData col10 = new ColumnMetaData();
        col10.setColIndex(colIndex);
        col10.setColName("REF_GENERATION");
        col10.setColType(Types.NCHAR);
        return col10;
    }

    protected ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types, Connection connection) throws SQLException {
        if (catalog == null || catalog.isEmpty())
            return null;
        if (!isAvailableCatalog(connection, catalog))
            return new EmptyResultSet();

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set column metadata list
        resultSet.setColumnMetaDataList(buildGetTablesColumnMetaDataList());
        // set row data
        List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("use " + catalog);
            ResultSet tables = stmt.executeQuery("show tables");
            while (tables.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                rowData.setStringValue(1, catalog);                                     //TABLE_CAT
                rowData.setStringValue(2, null);                                 //TABLE_SCHEM
                rowData.setStringValue(3, tables.getString("table_name"));  //TABLE_NAME
                rowData.setStringValue(4, "TABLE");                              //TABLE_TYPE
                rowData.setStringValue(5, "");                                   //REMARKS
                rowDataList.add(rowData);
            }
            ResultSet stables = stmt.executeQuery("show stables");
            while (stables.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                rowData.setStringValue(1, catalog);                                  //TABLE_CAT
                rowData.setStringValue(2, null);                              //TABLE_SCHEM
                rowData.setStringValue(3, stables.getString("name"));    //TABLE_NAME
                rowData.setStringValue(4, "TABLE");                           //TABLE_TYPE
                rowData.setStringValue(5, "STABLE");                          //REMARKS
                rowDataList.add(rowData);
            }
            resultSet.setRowDataList(rowDataList);
        }
        return resultSet;
    }

    private ColumnMetaData buildTableTypeMeta(int colIndex) {
        ColumnMetaData col4 = new ColumnMetaData();
        col4.setColIndex(colIndex);
        col4.setColName("TABLE_TYPE");
        col4.setColType(Types.NCHAR);
        return col4;
    }

    public ResultSet getSchemas() throws SQLException {
        return getEmptyResultSet();
    }

    public abstract ResultSet getCatalogs() throws SQLException;

    private List<ColumnMetaData> buildTableTypesColumnMetadataList() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableTypeMeta(1));
        return columnMetaDataList;
    }

    public ResultSet getTableTypes() throws SQLException {
        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set up ColumnMetaDataList
        resultSet.setColumnMetaDataList(buildTableTypesColumnMetadataList());

        // set up rowDataList
        List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
        TSDBResultSetRowData rowData = new TSDBResultSetRowData(1);
        rowData.setStringValue(1, "TABLE");
        rowDataList.add(rowData);
        rowData = new TSDBResultSetRowData(1);
        rowData.setStringValue(1, "STABLE");
        rowDataList.add(rowData);
        resultSet.setRowDataList(rowDataList);

        return resultSet;
    }

    public abstract ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException;

    protected ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern, Connection conn) {
        if (catalog == null || catalog.isEmpty())
            return null;
        if (!isAvailableCatalog(conn, catalog))
            return new EmptyResultSet();

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set up ColumnMetaDataList
        resultSet.setColumnMetaDataList(buildGetColumnsColumnMetaDataList());
        // set up rowDataList
        List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("describe " + catalog + "." + tableNamePattern);
            int rowIndex = 0;
            while (rs.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(24);
                // set TABLE_CAT
                rowData.setStringValue(1, catalog);
                // set TABLE_SCHEM
                rowData.setStringValue(2, null);
                // set TABLE_NAME
                rowData.setStringValue(3, tableNamePattern);
                // set COLUMN_NAME
                rowData.setStringValue(4, rs.getString("Field"));
                // set DATA_TYPE
                String typeName = rs.getString("Type");
                rowData.setIntValue(5, TSDBConstants.typeName2JdbcType(typeName));
                // set TYPE_NAME
                rowData.setStringValue(6, typeName);
                // set COLUMN_SIZE
                int length = rs.getInt("Length");
                rowData.setIntValue(7, calculateColumnSize(typeName, precision, length));
                // set BUFFER LENGTH
                rowData.setStringValue(8, null);
                // set DECIMAL_DIGITS
                Integer decimalDigits = calculateDecimalDigits(typeName);
                if (decimalDigits != null) {
                    rowData.setIntValue(9, decimalDigits);
                } else {
                    rowData.setStringValue(9, null);
                }
                // set NUM_PREC_RADIX
                rowData.setIntValue(10, 10);
                // set NULLABLE
                rowData.setIntValue(11, isNullable(rowIndex, typeName));
                // set REMARKS
                String note = rs.getString("Note");
                rowData.setStringValue(12, note.trim().isEmpty() ? null : note);
                rowDataList.add(rowData);
                rowIndex++;
            }
            resultSet.setRowDataList(rowDataList);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    private int isNullable(int index, String typeName) {
        if (index == 0 && "TIMESTAMP".equals(typeName))
            return DatabaseMetaData.columnNoNulls;
        return DatabaseMetaData.columnNullable;
    }

    private Integer calculateColumnSize(String typeName, String precisionType, int length) {
        switch (typeName) {
            case "TIMESTAMP":
                return precisionType.equals("ms") ? TSDBConstants.TIMESTAMP_MS_PRECISION : TSDBConstants.TIMESTAMP_US_PRECISION;
            case "BOOL":
                return TSDBConstants.BOOLEAN_PRECISION;
            case "TINYINT":
                return TSDBConstants.TINYINT_PRECISION;
            case "SMALLINT":
                return TSDBConstants.SMALLINT_PRECISION;
            case "INT":
                return TSDBConstants.INT_PRECISION;
            case "BIGINT":
                return TSDBConstants.BIGINT_PRECISION;
            case "FLOAT":
                return TSDBConstants.FLOAT_PRECISION;
            case "DOUBLE":
                return TSDBConstants.DOUBLE_PRECISION;
            case "NCHAR":
            case "BINARY":
                return length;
            default:
                return null;
        }
    }

    private Integer calculateDecimalDigits(String typeName) {
        switch (typeName) {
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
                return 0;
            default:
                return null;
        }
    }

    private ColumnMetaData buildTableCatalogMeta(int colIndex) {
        ColumnMetaData col1 = new ColumnMetaData();
        col1.setColIndex(colIndex);
        col1.setColName("TABLE_CAT");
        col1.setColType(Types.NCHAR);
        return col1;
    }

    private ColumnMetaData buildTableSchemaMeta(int colIndex) {
        ColumnMetaData col2 = new ColumnMetaData();
        col2.setColIndex(colIndex);
        col2.setColName("TABLE_SCHEM");
        col2.setColType(Types.NCHAR);
        return col2;
    }

    private ColumnMetaData buildTableNameMeta(int colIndex) {
        ColumnMetaData col3 = new ColumnMetaData();
        col3.setColIndex(colIndex);
        col3.setColName("TABLE_NAME");
        col3.setColSize(193);
        col3.setColType(Types.NCHAR);
        return col3;
    }

    private ColumnMetaData buildColumnNameMeta(int colIndex) {
        ColumnMetaData col4 = new ColumnMetaData();
        col4.setColIndex(colIndex);
        col4.setColName("COLUMN_NAME");
        col4.setColSize(65);
        col4.setColType(Types.NCHAR);
        return col4;
    }

    private ColumnMetaData buildDataTypeMeta(int colIndex) {
        ColumnMetaData col5 = new ColumnMetaData();
        col5.setColIndex(colIndex);
        col5.setColName("DATA_TYPE");
        col5.setColType(Types.INTEGER);
        return col5;
    }

    private ColumnMetaData buildColumnSizeMeta() {
        ColumnMetaData col7 = new ColumnMetaData();
        col7.setColIndex(7);
        col7.setColName("COLUMN_SIZE");
        col7.setColType(Types.INTEGER);
        return col7;
    }

    private ColumnMetaData buildBufferLengthMeta() {
        ColumnMetaData col8 = new ColumnMetaData();
        col8.setColIndex(8);
        col8.setColName("BUFFER_LENGTH");
        return col8;
    }

    private ColumnMetaData buildDecimalDigitsMeta() {
        ColumnMetaData col9 = new ColumnMetaData();
        col9.setColIndex(9);
        col9.setColName("DECIMAL_DIGITS");
        col9.setColType(Types.INTEGER);
        return col9;
    }

    private ColumnMetaData buildNumPrecRadixMeta() {
        ColumnMetaData col10 = new ColumnMetaData();
        col10.setColIndex(10);
        col10.setColName("NUM_PREC_RADIX");
        col10.setColType(Types.INTEGER);
        return col10;
    }

    private ColumnMetaData buildNullableMeta() {
        ColumnMetaData col11 = new ColumnMetaData();
        col11.setColIndex(11);
        col11.setColName("NULLABLE");
        col11.setColType(Types.INTEGER);
        return col11;
    }

    private ColumnMetaData buildRemarksMeta(int colIndex) {
        ColumnMetaData col12 = new ColumnMetaData();
        col12.setColIndex(colIndex);
        col12.setColName("REMARKS");
        col12.setColType(Types.NCHAR);
        return col12;
    }

    private ColumnMetaData buildColumnDefMeta() {
        ColumnMetaData col13 = new ColumnMetaData();
        col13.setColIndex(13);
        col13.setColName("COLUMN_DEF");
        col13.setColType(Types.NCHAR);
        return col13;
    }

    private ColumnMetaData buildSqlDataTypeMeta() {
        ColumnMetaData col14 = new ColumnMetaData();
        col14.setColIndex(14);
        col14.setColName("SQL_DATA_TYPE");
        col14.setColType(Types.INTEGER);
        return col14;
    }

    private ColumnMetaData buildSqlDatetimeSubMeta() {
        ColumnMetaData col15 = new ColumnMetaData();
        col15.setColIndex(15);
        col15.setColName("SQL_DATETIME_SUB");
        col15.setColType(Types.INTEGER);
        return col15;
    }

    private ColumnMetaData buildCharOctetLengthMeta() {
        ColumnMetaData col16 = new ColumnMetaData();
        col16.setColIndex(16);
        col16.setColName("CHAR_OCTET_LENGTH");
        col16.setColType(Types.INTEGER);
        return col16;
    }

    private ColumnMetaData buildOrdinalPositionMeta() {
        ColumnMetaData col17 = new ColumnMetaData();
        col17.setColIndex(17);
        col17.setColName("ORDINAL_POSITION");
        col17.setColType(Types.INTEGER);
        return col17;
    }

    private ColumnMetaData buildIsNullableMeta() {
        ColumnMetaData col18 = new ColumnMetaData();
        col18.setColIndex(18);
        col18.setColName("IS_NULLABLE");
        col18.setColType(Types.NCHAR);
        return col18;
    }

    private ColumnMetaData buildScopeCatalogMeta() {
        ColumnMetaData col19 = new ColumnMetaData();
        col19.setColIndex(19);
        col19.setColName("SCOPE_CATALOG");
        col19.setColType(Types.NCHAR);
        return col19;
    }

    private ColumnMetaData buildScopeSchemaMeta() {
        ColumnMetaData col20 = new ColumnMetaData();
        col20.setColIndex(20);
        col20.setColName("SCOPE_SCHEMA");
        col20.setColType(Types.NCHAR);
        return col20;
    }

    private ColumnMetaData buildScopeTableMeta() {
        ColumnMetaData col21 = new ColumnMetaData();
        col21.setColIndex(21);
        col21.setColName("SCOPE_TABLE");
        col21.setColType(Types.NCHAR);
        return col21;
    }

    private ColumnMetaData buildSourceDataTypeMeta() {
        ColumnMetaData col22 = new ColumnMetaData();
        col22.setColIndex(22);
        col22.setColName("SOURCE_DATA_TYPE");
        col22.setColType(TSDBConstants.TSDB_DATA_TYPE_SMALLINT);
        return col22;
    }

    private ColumnMetaData buildIsAutoIncrementMeta() {
        ColumnMetaData col23 = new ColumnMetaData();
        col23.setColIndex(23);
        col23.setColName("IS_AUTOINCREMENT");
        col23.setColType(Types.NCHAR);
        return col23;
    }

    private ColumnMetaData buildIsGeneratedColumnMeta() {
        ColumnMetaData col24 = new ColumnMetaData();
        col24.setColIndex(24);
        col24.setColName("IS_GENERATEDCOLUMN");
        col24.setColType(Types.NCHAR);
        return col24;
    }

    private List<ColumnMetaData> buildGetColumnsColumnMetaDataList() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableCatalogMeta(1));        //1. TABLE_CAT
        columnMetaDataList.add(buildTableSchemaMeta(2));         //2. TABLE_SCHEMA
        columnMetaDataList.add(buildTableNameMeta(3));           //3. TABLE_NAME
        columnMetaDataList.add(buildColumnNameMeta(4));          //4. COLUMN_NAME
        columnMetaDataList.add(buildDataTypeMeta(5));            //5. DATA_TYPE
        columnMetaDataList.add(buildTypeNameMeta(6));            //6. TYPE_NAME
        columnMetaDataList.add(buildColumnSizeMeta());                    //7. COLUMN_SIZE
        columnMetaDataList.add(buildBufferLengthMeta());                  //8. BUFFER_LENGTH, not used
        columnMetaDataList.add(buildDecimalDigitsMeta());       //9. DECIMAL_DIGITS
        columnMetaDataList.add(buildNumPrecRadixMeta());        //10. NUM_PREC_RADIX
        columnMetaDataList.add(buildNullableMeta());            //11. NULLABLE
        columnMetaDataList.add(buildRemarksMeta(12));  //12. REMARKS
        columnMetaDataList.add(buildColumnDefMeta());           //13. COLUMN_DEF
        columnMetaDataList.add(buildSqlDataTypeMeta());         //14. SQL_DATA_TYPE
        columnMetaDataList.add(buildSqlDatetimeSubMeta());      //15. SQL_DATETIME_SUB
        columnMetaDataList.add(buildCharOctetLengthMeta());     //16. CHAR_OCTET_LENGTH
        columnMetaDataList.add(buildOrdinalPositionMeta());     //17. ORDINAL_POSITION
        columnMetaDataList.add(buildIsNullableMeta());          //18. IS_NULLABLE
        columnMetaDataList.add(buildScopeCatalogMeta());        //19. SCOPE_CATALOG
        columnMetaDataList.add(buildScopeSchemaMeta());         //20. SCOPE_SCHEMA
        columnMetaDataList.add(buildScopeTableMeta());          //21. SCOPE_TABLE
        columnMetaDataList.add(buildSourceDataTypeMeta());      //22. SOURCE_DATA_TYPE
        columnMetaDataList.add(buildIsAutoIncrementMeta());     //23. IS_AUTOINCREMENT
        columnMetaDataList.add(buildIsGeneratedColumnMeta());   //24. IS_GENERATEDCOLUMN
        return columnMetaDataList;
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    public abstract ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException;

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getTypeInfo() throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
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

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return getEmptyResultSet();
    }

    public abstract Connection getConnection() throws SQLException;

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

    public abstract ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException;

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
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
        return getEmptyResultSet();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        //TODO: see https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html#setClientInfo-java.lang.String-java.lang.String-
        return getEmptyResultSet();
    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    private ResultSet getEmptyResultSet() {
        return new EmptyResultSet();
    }

    protected ResultSet getCatalogs(Connection conn) throws SQLException {
        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set up ColumnMetaDataList
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableCatalogMeta(1));       // 1. TABLE_CAT
        resultSet.setColumnMetaDataList(columnMetaDataList);

        try (Statement stmt = conn.createStatement()) {
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("show databases");
            while (rs.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(1);
                rowData.setStringValue(1, rs.getString("name"));
                rowDataList.add(rowData);
            }
            resultSet.setRowDataList(rowDataList);
        }
        return resultSet;
    }


    protected ResultSet getPrimaryKeys(String catalog, String schema, String table, Connection conn) throws SQLException {
        if (catalog == null || catalog.isEmpty())
            return null;
        if (!isAvailableCatalog(conn, catalog))
            return new EmptyResultSet();

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        try (Statement stmt = conn.createStatement()) {
            // set up ColumnMetaDataList
            resultSet.setColumnMetaDataList(buildGetPrimaryKeysMetadataList());
            // set rowData
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("describe " + catalog + "." + table);
            rs.next();
            TSDBResultSetRowData rowData = new TSDBResultSetRowData(6);
            rowData.setStringValue(1, catalog);
            rowData.setStringValue(2, null);
            rowData.setStringValue(3, table);
            String primaryKey = rs.getString("Field");
            rowData.setStringValue(4, primaryKey);
            rowData.setShortValue(5, (short) 1);
            rowData.setStringValue(6, primaryKey);
            rowDataList.add(rowData);
            resultSet.setRowDataList(rowDataList);
        }
        return resultSet;
    }

    private List<ColumnMetaData> buildGetPrimaryKeysMetadataList() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableCatalogMeta(1));   // 1. TABLE_CAT
        columnMetaDataList.add(buildTableSchemaMeta(2));    // 2. TABLE_SCHEM
        columnMetaDataList.add(buildTableNameMeta(3));      // 3. TABLE_NAME
        columnMetaDataList.add(buildColumnNameMeta(4));     // 4. COLUMN_NAME
        columnMetaDataList.add(buildKeySeqMeta(5));         // 5. KEY_SEQ
        columnMetaDataList.add(buildPrimaryKeyNameMeta(6)); // 6. PK_NAME
        return columnMetaDataList;
    }

    private ColumnMetaData buildKeySeqMeta(int colIndex) {
        ColumnMetaData col5 = new ColumnMetaData();
        col5.setColIndex(colIndex);
        col5.setColName("KEY_SEQ");
        col5.setColType(Types.SMALLINT);
        return col5;
    }

    private ColumnMetaData buildPrimaryKeyNameMeta(int colIndex) {
        ColumnMetaData col6 = new ColumnMetaData();
        col6.setColIndex(colIndex);
        col6.setColName("PK_NAME");
        col6.setColType(Types.NCHAR);
        return col6;
    }

    private boolean isAvailableCatalog(Connection connection, String catalog) {
        try (Statement stmt = connection.createStatement()) {
            ResultSet databases = stmt.executeQuery("show databases");
            while (databases.next()) {
                String dbname = databases.getString("name");
                this.database = dbname;
                this.precision = databases.getString("precision");
                if (dbname.equalsIgnoreCase(catalog))
                    return true;
            }
            databases.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    protected ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern, Connection conn) throws SQLException {
        if (catalog == null || catalog.isEmpty())
            return null;

        if (!isAvailableCatalog(conn, catalog)) {
            return new EmptyResultSet();
        }

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        try (Statement stmt = conn.createStatement()) {
            // set up ColumnMetaDataList
            resultSet.setColumnMetaDataList(buildGetSuperTablesColumnMetaDataList());
            // set result set row data
            stmt.execute("use " + catalog);
            ResultSet rs = stmt.executeQuery("show tables like '" + tableNamePattern + "'");
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
            while (rs.next()) {
                TSDBResultSetRowData rowData = new TSDBResultSetRowData(4);
                rowData.setStringValue(1, catalog);
                rowData.setStringValue(2, null);
                rowData.setStringValue(3, rs.getString("table_name"));
                rowData.setStringValue(4, rs.getString("stable_name"));
                rowDataList.add(rowData);
            }
            resultSet.setRowDataList(rowDataList);
        }
        return resultSet;
    }

    private List<ColumnMetaData> buildGetSuperTablesColumnMetaDataList() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableCatalogMeta(1));       // 1. TABLE_CAT
        columnMetaDataList.add(buildTableSchemaMeta(2));        // 2. TABLE_SCHEM
        columnMetaDataList.add(buildTableNameMeta(3));          // 3. TABLE_NAME
        columnMetaDataList.add(buildSuperTableNameMeta(4));     // 4. SUPERTABLE_NAME
        return columnMetaDataList;
    }

    private ColumnMetaData buildSuperTableNameMeta(int colIndex) {
        ColumnMetaData col4 = new ColumnMetaData();
        col4.setColIndex(colIndex);
        col4.setColName("SUPERTABLE_NAME");
        col4.setColType(Types.NCHAR);
        return col4;
    }

}