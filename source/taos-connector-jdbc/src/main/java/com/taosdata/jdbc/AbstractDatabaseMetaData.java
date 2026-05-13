package com.taosdata.jdbc;

import com.google.common.collect.Lists;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.utils.ProductUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.WSConnection;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractDatabaseMetaData extends WrapperImpl implements DatabaseMetaData {
    public static final String NUMERIC_FUNCTIONS = "ABS,ACOS,ASIN,ATAN,CEIL,COS,FLOOR,LOG,POW,ROUND,SIN,SQRT,TAN";
    public static final String STRING_FUNCTIONS = "CHAR_LENGTH,CONCAT,CONCAT_WS,LENGTH,LOWER,LTRIM,RTRIM,SUBSTR,UPPER";
    public static final String SYSTEM_FUNCTIONS = "DATABASE,CLIENT_VERSION,SERVER_VERSION,SERVER_STATUS,CURRENT_USER";
    public static final String TIME_DATE_FUNCTIONS = "NOW,TIMEDIFF,TIMETRUNCATE,TIMEZONE,TODAY";
    private static final Set<String> tableTypeSet = Stream.of("TABLE", "STABLE", "VIEW").collect(Collectors.toSet());

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
        return ProductUtil.getProductName();
    }

    public String getDatabaseProductVersion() throws SQLException {

        Connection conn = getConnection();
        if (conn == null || conn.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        String version = null;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select server_version()  as version")) {
            while (rs.next()) {
                version = rs.getString("version");
            }
        }

        if (StringUtils.isEmpty(version)) {
            return ProductUtil.getProductVersion();
        } else {
            return version;
        }
    }

    public String getDriverVersion() throws SQLException {
        return ProductUtil.getDriverVersion();
    }

    public int getDriverMajorVersion() {
        return ProductUtil.getDriverMajorVersion();
    }

    public int getDriverMinorVersion() {
        return ProductUtil.getDriverMinorVersion();
    }

    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;   //像database、table这些对象的标识符，在存储时是否采用大小写混合的模式
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

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    public String getSQLKeywords() throws SQLException {
        return null;
    }

    public String getNumericFunctions() throws SQLException {
        return NUMERIC_FUNCTIONS;
    }

    public String getStringFunctions() throws SQLException {
        return STRING_FUNCTIONS;
    }

    public String getSystemFunctions() throws SQLException {
        return SYSTEM_FUNCTIONS;
    }

    public String getTimeDateFunctions() throws SQLException {
        return TIME_DATE_FUNCTIONS;
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
        col6.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col6;
    }

    private ColumnMetaData buildTypeSchemaMeta(int colIndex) {
        ColumnMetaData col7 = new ColumnMetaData();
        col7.setColIndex(colIndex);
        col7.setColName("TYPE_SCHEM");
        col7.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col7;
    }

    private ColumnMetaData buildTypeNameMeta(int colIndex) {
        ColumnMetaData col8 = new ColumnMetaData();
        col8.setColIndex(colIndex);
        col8.setColName("TYPE_NAME");
        col8.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col8;
    }

    private ColumnMetaData buildSelfReferencingColName(int colIndex) {
        ColumnMetaData col9 = new ColumnMetaData();
        col9.setColIndex(colIndex);
        col9.setColName("SELF_REFERENCING_COL_NAME");
        col9.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col9;
    }

    private ColumnMetaData buildRefGenerationMeta(int colIndex) {
        ColumnMetaData col10 = new ColumnMetaData();
        col10.setColIndex(colIndex);
        col10.setColName("REF_GENERATION");
        col10.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col10;
    }

    protected ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types, Connection connection) throws SQLException {
        String dbHelperStr = "";
        String tableHelperStr = "";

        if (connection instanceof WSConnection) {
            WSConnection wsConnection = (WSConnection) connection;
            // BI mode, only query user tables, do not query subtables
            if (wsConnection.getParam().getConnectMode() == ConnectionParam.CONNECT_MODE_BI) {
                dbHelperStr = "user";
                tableHelperStr = "normal ";
            }
        }

        if (!StringUtils.isEmpty(catalog) && !isAvailableCatalog(connection, catalog)) {
            return new EmptyResultSet();
        }

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set column metadata list
        resultSet.setColumnMetaDataList(buildGetTablesColumnMetaDataList());
        // set row data
        List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            List<String> dbs = new ArrayList<>();
            if (!StringUtils.isEmpty(catalog)) {
                dbs.add(catalog);
            } else {
                try (ResultSet dbRs = stmt.executeQuery("show " + dbHelperStr + " databases")) {
                    while (dbRs.next()) {
                        dbs.add(dbRs.getString("name"));
                    }
                }
            }
            if (dbs.isEmpty()) {
                return new EmptyResultSet();
            }
            for (String db : dbs) {

                Set<String> tempTableTypeSet;
                if (types == null || types.length == 0) {
                    tempTableTypeSet = tableTypeSet;
                } else {
                    tempTableTypeSet = new HashSet<>(Arrays.asList(types));
                }

                StringBuilder sql = new StringBuilder().append("show ")
                        .append(tableHelperStr)
                        .append(getIdentifierQuoteString())
                        .append(db)
                        .append(getIdentifierQuoteString())
                        .append(".tables ");
                StringBuilder Ssql = new StringBuilder().append("show ")
                        .append(getIdentifierQuoteString())
                        .append(db)
                        .append(getIdentifierQuoteString())
                        .append(".stables ");
                StringBuilder vsql = new StringBuilder().append("show ")
                        .append(getIdentifierQuoteString())
                        .append(db)
                        .append(getIdentifierQuoteString())
                        .append(".views ");

                if (!StringUtils.isEmpty(tableNamePattern)) {
                    sql.append("like '").append(tableNamePattern).append("'");
                    Ssql.append("like '").append(tableNamePattern).append("'");
                    vsql.append("like '").append(tableNamePattern).append("'");
                }

                if (tempTableTypeSet.contains("TABLE")) {
                    try (ResultSet rs = stmt.executeQuery(sql.toString())) {
                        while (rs.next()) {
                            TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                            rowData.setStringValue(1, db);                                     //TABLE_CAT
                            rowData.setStringValue(2, null);                                 //TABLE_SCHEM
                            rowData.setStringValue(3, rs.getString("table_name"));  //TABLE_NAME
                            rowData.setStringValue(4, "TABLE");                              //TABLE_TYPE
                            rowData.setStringValue(5, "");                                   //REMARKS
                            rowDataList.add(rowData);
                        }
                    }
                    try (ResultSet rs = stmt.executeQuery(Ssql.toString())) {
                        while (rs.next()) {
                            TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                            rowData.setStringValue(1, db);                                  //TABLE_CAT
                            rowData.setStringValue(2, null);                              //TABLE_SCHEM
                            rowData.setStringValue(3, rs.getString("stable_name"));    //TABLE_NAME
                            rowData.setStringValue(4, "TABLE");                           //TABLE_TYPE
                            rowData.setStringValue(5, "STABLE");                          //REMARKS
                            rowDataList.add(rowData);
                        }
                    }
                }
                if (tempTableTypeSet.contains("VIEW")) {
                    try (ResultSet rs = stmt.executeQuery(vsql.toString())) {
                        while (rs.next()) {
                            TSDBResultSetRowData rowData = new TSDBResultSetRowData(10);
                            rowData.setStringValue(1, db);                                  //TABLE_CAT
                            rowData.setStringValue(2, null);                          //TABLE_SCHEM
                            rowData.setStringValue(3, rs.getString("view_name"));    //TABLE_NAME
                            rowData.setStringValue(4, "VIEW");                           //TABLE_TYPE
                            rowData.setStringValue(5, "VIEW");                          //REMARKS
                            rowDataList.add(rowData);
                        }
                    }
                }
            }
            resultSet.setRowDataList(rowDataList);
        }
        return resultSet;
    }

    private ColumnMetaData buildTableTypeMeta(int colIndex) {
        ColumnMetaData col4 = new ColumnMetaData();
        col4.setColIndex(colIndex);
        col4.setColName("TABLE_TYPE");
        col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col4;
    }

    public ResultSet getSchemas() {
        return getEmptyResultSet();
    }

    private List<ColumnMetaData> buildTableTypesColumnMetadataList() {
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(buildTableTypeMeta(1));
        return columnMetaDataList;
    }

    public ResultSet getTableTypes() throws SQLException {
        Connection connection = getConnection();
        if (connection == null || connection.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set up ColumnMetaDataList
        resultSet.setColumnMetaDataList(buildTableTypesColumnMetadataList());

        // set up rowDataList
        List<TSDBResultSetRowData> rowDataList = Lists.newArrayListWithExpectedSize(tableTypeSet.size());
        for (String tableType : tableTypeSet) {
            TSDBResultSetRowData rowData = new TSDBResultSetRowData(1);
            rowData.setStringValue(1, tableType);
            rowDataList.add(rowData);
        }

        resultSet.setRowDataList(rowDataList);
        return resultSet;
    }

    private Map<String, String> getCatalogMate(Connection connection, String catalog) throws SQLException {
        Map<String, String> result = new HashMap<>();
        try (Statement stmt = connection.createStatement();
             ResultSet databases = stmt.executeQuery("select name, `precision` as `precision` from information_schema.ins_databases")) {
            while (databases.next()) {
                String dbname = databases.getString("name");
                String precision = databases.getString("precision");
                if (catalog == null) {
                    result.put(dbname, precision);
                } else {
                    if (dbname.equalsIgnoreCase(catalog))
                        result.put(dbname, precision);
                }
            }
        }
        return result;
    }

    // database - stables
    private Map<String, Set<String>> getSTableMate(Connection connection, String sTable, String db) throws SQLException {
        Map<String, Set<String>> result = new HashMap<>();
        String sql = "select stable_name, db_name from information_schema.ins_stables where 1 = 1 ";
        if (sTable != null)
            sql += " and stable_name='" + sTable + "'";
        if (db != null)
            sql += " and db_name= '" + db + "'";
        try (Statement stmt = connection.createStatement();
             ResultSet databases = stmt.executeQuery(sql)) {
            while (databases.next()) {
                String name = databases.getString("stable_name");
                String dbName = databases.getString("db_name");
                Set<String> stables = result.computeIfAbsent(dbName, k -> new HashSet<>());
                stables.add(name);
            }
        }
        return result;
    }

    private Map<String, Set<String>> getTableMate(Connection connection, String table, String db) throws SQLException {
        Map<String, Set<String>> result = new HashMap<>();
        String sql = "select table_name, db_name, stable_name from information_schema.ins_tables where 1 = 1 ";
        if (table != null)
            sql += " and table_name='" + table + "'";
        if (db != null)
            sql += " and db_name='" + db + "'";
        try (Statement stmt = connection.createStatement();
             ResultSet tables = stmt.executeQuery(sql)) {
            while (tables.next()) {
                String name = tables.getString("table_name");
                String dbName = tables.getString("db_name");
                Set<String> set = result.computeIfAbsent(dbName, k -> new HashSet<>());
                set.add(name);
            }
        }
        return result;
    }

    private Map<String, Set<String>> getViewMate(Connection connection, String viewName, String db) throws SQLException {
        Map<String, Set<String>> result = new HashMap<>();
        String sql = "select view_name, db_name from information_schema.ins_views where 1 = 1 ";
        if (viewName != null)
            sql += " and view_name='" + viewName + "'";
        if (db != null)
            sql += " and db_name= '" + db + "'";
        try (Statement stmt = connection.createStatement();
             ResultSet databases = stmt.executeQuery(sql)) {
            while (databases.next()) {
                String name = databases.getString("view_name");
                String dbName = databases.getString("db_name");
                Set<String> views = result.computeIfAbsent(dbName, k -> new HashSet<>());
                views.add(name);
            }
        }
        return result;
    }

    // super table
    private void show2RowData(ResultSet rs, List<TSDBResultSetRowData> rowDataList, String precision, String db, String tableName, String colName) throws SQLException {
        int rowIndex = 0;
        while (rs.next()) {
//            table_name, db_name, table_type, col_name as field, col_type as type, col_length as length
            TSDBResultSetRowData rowData = new TSDBResultSetRowData(24);
            // set TABLE_CAT
            rowData.setStringValue(1, db);
            // set TABLE_SCHEM
            rowData.setStringValue(2, null);
            // set TABLE_NAME
            rowData.setStringValue(3, tableName);
            // set COLUMN_NAME
            String field = rs.getString("field");
            if (colName == null || colName.equals("%") || colName.equals(field)) {
                rowData.setStringValue(4, field);
            } else {
                continue;
            }
            // set DATA_TYPE
            String typeName = rs.getString("type");
            rowData.setIntValue(5, DataType.getDataType(typeName).getJdbcTypeValue());
            // set TYPE_NAME
            rowData.setStringValue(6, typeName);
            // set COLUMN_SIZE
            int length = rs.getInt("length");
            int size = DataType.calculateColumnSize(typeName, precision, length);
            if (size != -1) {
                rowData.setIntValue(7, size);
            } else {
                rowData.setString(7, null);
            }
            // set BUFFER LENGTH
            rowData.setStringValue(8, null);
            // set DECIMAL_DIGITS
            Integer decimalDigits = DataType.calculateDecimalDigits(typeName);
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
            String note = rs.getString("note");
            rowData.setStringValue(12, note.trim().isEmpty() ? null : note);
            rowDataList.add(rowData);
            rowIndex++;
        }
    }

    Pattern pattern = Pattern.compile("\\((\\d+)\\)");

    // normal table or child table
    private void colResultSet2RowData(ResultSet rs, List<TSDBResultSetRowData> rowDataList, Map<String, String> precision) throws SQLException {
        int rowIndex = 0;
        while (rs.next()) {
//            table_name, db_name, table_type, col_name as field, col_type as type, col_length as length
            TSDBResultSetRowData rowData = new TSDBResultSetRowData(24);
            // set TABLE_CAT
            String db = rs.getString("db_name");
            rowData.setStringValue(1, db);
            // set TABLE_SCHEM
            rowData.setStringValue(2, null);
            // set TABLE_NAME
            String tableName = rs.getString("table_name");
            rowData.setStringValue(3, tableName);
            // set COLUMN_NAME
            rowData.setStringValue(4, rs.getString("col_name"));
            // set DATA_TYPE
            String typeName = rs.getString("col_type");
            int length = 0;
            if (typeName.startsWith("VARCHAR")) {
                rowData.setIntValue(5, DataType.getDataType("VARCHAR").getJdbcTypeValue());
                length = rs.getInt("col_length") - 2;
                typeName = "VARCHAR";
            } else if (typeName.startsWith("NCHAR")) {
                rowData.setIntValue(5, DataType.getDataType("NCHAR").getJdbcTypeValue());
                Matcher matcher = pattern.matcher(typeName);
                if (matcher.find()) {
                    length = Integer.parseInt(matcher.group(1));
                }
                typeName = "NCHAR";
            } else {
                rowData.setIntValue(5, DataType.getDataType(typeName).getJdbcTypeValue());
                length = rs.getInt("col_length");
            }
            // set TYPE_NAME
            rowData.setStringValue(6, typeName);
            // set COLUMN_SIZE
            String p = precision.get(db);
            int size = DataType.calculateColumnSize(typeName, p, length);
            if (size != -1) {
                rowData.setIntValue(7, size);
            } else {
                rowData.setString(7, null);
            }
            // set BUFFER LENGTH
            rowData.setStringValue(8, null);
            // set DECIMAL_DIGITS
            Integer decimalDigits = DataType.calculateDecimalDigits(typeName);
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
            rowData.setStringValue(12, null);
            rowDataList.add(rowData);
            rowIndex++;
        }
    }

    // child table
    private void tagResultSet2RowData(ResultSet rs, List<TSDBResultSetRowData> rowDataList, Map<String, String> precision) throws SQLException {
        int rowIndex = 0;
        while (rs.next()) {
            TSDBResultSetRowData rowData = new TSDBResultSetRowData(24);
            // set TABLE_CAT
            String db = rs.getString("db_name");
            rowData.setStringValue(1, db);
            // set TABLE_SCHEM
            rowData.setStringValue(2, null);
            // set TABLE_NAME
            rowData.setStringValue(3, rs.getString("table_name"));
            // set COLUMN_NAME
            rowData.setStringValue(4, rs.getString("tag_name"));
            // set DATA_TYPE
            String typeName = rs.getString("tag_type");
            int length = 0;
            if (typeName.startsWith("VARCHAR")) {
                rowData.setIntValue(5, DataType.getDataType("VARCHAR").getJdbcTypeValue());
                Matcher matcher = pattern.matcher(typeName);
                if (matcher.find()) {
                    length = Integer.parseInt(matcher.group(1));
                }
                typeName = "VARCHAR";
            } else if (typeName.startsWith("NCHAR")) {
                rowData.setIntValue(5, DataType.getDataType("NCHAR").getJdbcTypeValue());
                Matcher matcher = pattern.matcher(typeName);
                if (matcher.find()) {
                    length = Integer.parseInt(matcher.group(1));
                }
                typeName = "NCHAR";
            } else {
                rowData.setIntValue(5, DataType.getDataType(typeName).getJdbcTypeValue());
                length = 0;
            }
            // set TYPE_NAME
            rowData.setStringValue(6, typeName);
            // set COLUMN_SIZE
            String p = precision.get(db);
            int size = DataType.calculateColumnSize(typeName, p, length);
            if (size != -1) {
                rowData.setIntValue(7, size);
            } else {
                rowData.setString(7, null);
            }
            // set BUFFER LENGTH
            rowData.setStringValue(8, null);
            // set DECIMAL_DIGITS
            Integer decimalDigits = DataType.calculateDecimalDigits(typeName);
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
            rowData.setStringValue(12, null);
            rowDataList.add(rowData);
            rowIndex++;
        }
    }

    protected ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern, Connection conn) throws SQLException {
        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        // set up ColumnMetaDataList
        resultSet.setColumnMetaDataList(buildGetColumnsColumnMetaDataList());
        // set up rowDataList
        List<TSDBResultSetRowData> rowDataList = new ArrayList<>();

        if (catalog == null || catalog.equals("%")) {
            Map<String, String> precisions = getCatalogMate(conn, null);
            if (tableNamePattern == null || tableNamePattern.equals("%")) {
                if (columnNamePattern == null || columnNamePattern.equals("%")) {
                    Map<String, Set<String>> sTableMate = getSTableMate(conn, null, null);
                    for (String db : sTableMate.keySet()) {
                        for (String s : sTableMate.get(db)) {
                            try (Statement stmt = conn.createStatement();
                                 ResultSet rs = stmt.executeQuery(generateDescribeSql(db, s))) {
                                show2RowData(rs, rowDataList, precisions.get(db), db, s, null);
                            }
                        }
                    }

                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, table_type, col_name, col_type, col_length " +
                                         "from information_schema.ins_columns")) {
                        colResultSet2RowData(rs, rowDataList, precisions);
                    }

                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, stable_name, tag_name, tag_type " +
                                         "from information_schema.ins_tags")) {
                        tagResultSet2RowData(rs, rowDataList, precisions);
                    }
                } else {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, table_type, col_name, col_type, col_length " +
                                         "from information_schema.ins_columns where col_name = '" + columnNamePattern + "'")) {
                        colResultSet2RowData(rs, rowDataList, precisions);
                    }
                    // stable tag not found
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, stable_name, tag_name, tag_type " +
                                         "from information_schema.ins_tags where tag_name = '" + columnNamePattern + "'")) {
                        tagResultSet2RowData(rs, rowDataList, precisions);
                    }
                }
            } else {
                Map<String, Set<String>> sTableMate = getSTableMate(conn, tableNamePattern, null);
                for (String db : sTableMate.keySet()) {
                    for (String s : sTableMate.get(db)) {
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(generateDescribeSql(db, s))) {
                            show2RowData(rs, rowDataList, precisions.get(db), db, s, columnNamePattern);
                        }
                    }
                }
                Map<String, Set<String>> tableMate = getTableMate(conn, tableNamePattern, null);
                for (Map.Entry<String, Set<String>> dbs : tableMate.entrySet()) {
                    for (String table : dbs.getValue()) {
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(generateDescribeSql(dbs.getKey(), table))) {
                            show2RowData(rs, rowDataList, precisions.get(dbs.getKey()), dbs.getKey(), table, columnNamePattern);
                        }
                    }
                }
            }
        } else {
            if (catalog.isEmpty())
                return new EmptyResultSet();
            Map<String, String> precisions = getCatalogMate(conn, catalog);
            if (precisions.get(catalog) == null)
                return new EmptyResultSet();
            if (tableNamePattern == null || tableNamePattern.equals("%")) {
                if (columnNamePattern == null || columnNamePattern.equals("%")) {
                    Map<String, Set<String>> stableMate = getSTableMate(conn, null, catalog);
                    for (String db : stableMate.keySet()) {
                        for (String s : stableMate.get(db)) {
                            try (Statement stmt = conn.createStatement();
                                 ResultSet rs = stmt.executeQuery(generateDescribeSql(catalog, s))) {
                                show2RowData(rs, rowDataList, precisions.get(catalog), catalog, s, null);
                            }
                        }
                    }

                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, table_type, col_name, col_type, col_length " +
                                         "from information_schema.ins_columns where db_name = '" + catalog + "'")) {
                        colResultSet2RowData(rs, rowDataList, precisions);
                    }

                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, stable_name, tag_name, tag_type " +
                                         "from information_schema.ins_tags where db_name = '" + catalog + "'")) {
                        tagResultSet2RowData(rs, rowDataList, precisions);
                    }
                } else {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, table_type, col_name, col_type, col_length " +
                                         "from information_schema.ins_columns where col_name = '" + columnNamePattern + "' and db_name = '" + catalog + "'")) {
                        colResultSet2RowData(rs, rowDataList, precisions);
                    }

                    // stable tag no found in result
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                                 "select table_name, db_name, stable_name, tag_name, tag_type " +
                                         "from information_schema.ins_tags where tag_name = '" + columnNamePattern + "' and db_name = '" + catalog + "'")) {
                        tagResultSet2RowData(rs, rowDataList, precisions);
                    }
                }
            } else {
                Map<String, Set<String>> sTableMate = getSTableMate(conn, tableNamePattern, catalog);
                Map<String, Set<String>> tableMate = getTableMate(conn, tableNamePattern, catalog);
                Map<String, Set<String>> viewMate = getViewMate(conn, tableNamePattern, catalog);

                List<Map<String, Set<String>>> mapList = new ArrayList<>(Arrays.asList(sTableMate, tableMate, viewMate));

                for (Map<String, Set<String>> tmpMap : mapList) {
                    for (Map.Entry<String, Set<String>> dbs : tmpMap.entrySet()) {
                        for (String table : dbs.getValue()) {
                            try (Statement stmt = conn.createStatement();
                                 ResultSet rs = stmt.executeQuery(generateDescribeSql(catalog, table))) {
                                show2RowData(rs, rowDataList, precisions.get(catalog), catalog, table, columnNamePattern);
                            }
                        }
                    }
                }
            }
        }
        resultSet.setRowDataList(rowDataList);
        return resultSet;
    }

    private int isNullable(int index, String typeName) {
        if (index == 0 && "TIMESTAMP".equals(typeName))
            return DatabaseMetaData.columnNoNulls;
        return DatabaseMetaData.columnNullable;
    }

    private ColumnMetaData buildTableCatalogMeta(int colIndex) {
        ColumnMetaData col1 = new ColumnMetaData();
        col1.setColIndex(colIndex);
        col1.setColName("TABLE_CAT");
        col1.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col1;
    }

    private ColumnMetaData buildTableSchemaMeta(int colIndex) {
        ColumnMetaData col2 = new ColumnMetaData();
        col2.setColIndex(colIndex);
        col2.setColName("TABLE_SCHEM");
        col2.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col2;
    }

    private ColumnMetaData buildTableNameMeta(int colIndex) {
        ColumnMetaData col3 = new ColumnMetaData();
        col3.setColIndex(colIndex);
        col3.setColName("TABLE_NAME");
        col3.setColSize(193);
        col3.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col3;
    }

    private ColumnMetaData buildColumnNameMeta(int colIndex) {
        ColumnMetaData col4 = new ColumnMetaData();
        col4.setColIndex(colIndex);
        col4.setColName("COLUMN_NAME");
        col4.setColSize(65);
        col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col4;
    }

    private ColumnMetaData buildDataTypeMeta(int colIndex) {
        ColumnMetaData col5 = new ColumnMetaData();
        col5.setColIndex(colIndex);
        col5.setColName("DATA_TYPE");
        col5.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col5;
    }

    private ColumnMetaData buildColumnSizeMeta() {
        ColumnMetaData col7 = new ColumnMetaData();
        col7.setColIndex(7);
        col7.setColName("COLUMN_SIZE");
        col7.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
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
        col9.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col9;
    }

    private ColumnMetaData buildNumPrecRadixMeta() {
        ColumnMetaData col10 = new ColumnMetaData();
        col10.setColIndex(10);
        col10.setColName("NUM_PREC_RADIX");
        col10.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col10;
    }

    private ColumnMetaData buildNullableMeta() {
        ColumnMetaData col11 = new ColumnMetaData();
        col11.setColIndex(11);
        col11.setColName("NULLABLE");
        col11.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col11;
    }

    private ColumnMetaData buildRemarksMeta(int colIndex) {
        ColumnMetaData col12 = new ColumnMetaData();
        col12.setColIndex(colIndex);
        col12.setColName("REMARKS");
        col12.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col12;
    }

    private ColumnMetaData buildColumnDefMeta() {
        ColumnMetaData col13 = new ColumnMetaData();
        col13.setColIndex(13);
        col13.setColName("COLUMN_DEF");
        col13.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col13;
    }

    private ColumnMetaData buildSqlDataTypeMeta() {
        ColumnMetaData col14 = new ColumnMetaData();
        col14.setColIndex(14);
        col14.setColName("SQL_DATA_TYPE");
        col14.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col14;
    }

    private ColumnMetaData buildSqlDatetimeSubMeta() {
        ColumnMetaData col15 = new ColumnMetaData();
        col15.setColIndex(15);
        col15.setColName("SQL_DATETIME_SUB");
        col15.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col15;
    }

    private ColumnMetaData buildCharOctetLengthMeta() {
        ColumnMetaData col16 = new ColumnMetaData();
        col16.setColIndex(16);
        col16.setColName("CHAR_OCTET_LENGTH");
        col16.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col16;
    }

    private ColumnMetaData buildOrdinalPositionMeta() {
        ColumnMetaData col17 = new ColumnMetaData();
        col17.setColIndex(17);
        col17.setColName("ORDINAL_POSITION");
        col17.setColType(TSDBConstants.TSDB_DATA_TYPE_INT);
        return col17;
    }

    private ColumnMetaData buildIsNullableMeta() {
        ColumnMetaData col18 = new ColumnMetaData();
        col18.setColIndex(18);
        col18.setColName("IS_NULLABLE");
        col18.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col18;
    }

    private ColumnMetaData buildScopeCatalogMeta() {
        ColumnMetaData col19 = new ColumnMetaData();
        col19.setColIndex(19);
        col19.setColName("SCOPE_CATALOG");
        col19.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col19;
    }

    private ColumnMetaData buildScopeSchemaMeta() {
        ColumnMetaData col20 = new ColumnMetaData();
        col20.setColIndex(20);
        col20.setColName("SCOPE_SCHEMA");
        col20.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col20;
    }

    private ColumnMetaData buildScopeTableMeta() {
        ColumnMetaData col21 = new ColumnMetaData();
        col21.setColIndex(21);
        col21.setColName("SCOPE_TABLE");
        col21.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
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
        col23.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col23;
    }

    private ColumnMetaData buildIsGeneratedColumnMeta() {
        ColumnMetaData col24 = new ColumnMetaData();
        col24.setColIndex(24);
        col24.setColName("IS_GENERATEDCOLUMN");
        col24.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
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
        return 3;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    public int getJDBCMajorVersion() throws SQLException {
        return 3;
    }

    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL99;
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

        String dbHelperStr = "";

        if (conn instanceof WSConnection) {
            WSConnection wsConnection = (WSConnection) conn;
            // BI mode, only query user databases, only query tables, not sub tables
            if (wsConnection.getParam().getConnectMode() == 1) {
                dbHelperStr = "user";
            }
        }

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("show " + dbHelperStr + " databases")) {
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
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
        if (catalog == null || catalog.isEmpty() || !isAvailableCatalog(conn, catalog))
            return new EmptyResultSet();

        DatabaseMetaDataResultSet resultSet = new DatabaseMetaDataResultSet();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(generateDescribeSql(catalog, table))) {
            // set up ColumnMetaDataList
            resultSet.setColumnMetaDataList(buildGetPrimaryKeysMetadataList());
            // set rowData
            List<TSDBResultSetRowData> rowDataList = new ArrayList<>();

            rs.next();
            TSDBResultSetRowData rowData = new TSDBResultSetRowData(6);
            rowData.setStringValue(1, catalog);
            rowData.setStringValue(2, null);
            rowData.setStringValue(3, table);
            String primaryKey = rs.getString("field");
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
        col5.setColType(TSDBConstants.TSDB_DATA_TYPE_SMALLINT);
        return col5;
    }

    private ColumnMetaData buildPrimaryKeyNameMeta(int colIndex) {
        ColumnMetaData col6 = new ColumnMetaData();
        col6.setColIndex(colIndex);
        col6.setColName("PK_NAME");
        col6.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col6;
    }

    private boolean isAvailableCatalog(Connection connection, String catalog) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet databases = stmt.executeQuery("select name, `precision` as `precision` from information_schema.ins_databases")) {
            while (databases.next()) {
                String dbname = databases.getString("name");
                if (dbname.equalsIgnoreCase(catalog))
                    return true;
            }
        }
        return false;
    }

    protected ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern, Connection conn) throws SQLException {
        return getEmptyResultSet();
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
        col4.setColType(TSDBConstants.TSDB_DATA_TYPE_NCHAR);
        return col4;
    }

    private String generateDescribeSql(String dbName, String tableName) throws SQLException {
        return "describe " +
                getIdentifierQuoteString() +
                dbName +
                getIdentifierQuoteString() +
                "." +
                getIdentifierQuoteString() +
                tableName +
                getIdentifierQuoteString();
    }


    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        Connection connection = getConnection();
        if (connection == null || connection.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return getTables(catalog, schemaPattern, tableNamePattern, types, connection);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Connection connection = getConnection();
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return getCatalogs(connection);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Connection connection = getConnection();
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern, connection);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        Connection connection = getConnection();
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return getPrimaryKeys(catalog, schema, table, connection);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        Connection connection = getConnection();
        if (connection == null || connection.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return getSuperTables(catalog, schemaPattern, tableNamePattern, connection);
    }
}
