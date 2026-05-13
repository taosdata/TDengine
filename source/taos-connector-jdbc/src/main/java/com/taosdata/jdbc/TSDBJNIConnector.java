package com.taosdata.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TaosInfo;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.MessageFormat;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_INVALID_VARIABLE;

/**
 * JNI connector
 */
public class TSDBJNIConnector {
    private static final Object LOCK = new Object();
    private static volatile boolean isInitialized;

    private final TaosInfo taosInfo = TaosInfo.getInstance();
    protected long taos = TSDBConstants.JNI_NULL_POINTER;     // Connection pointer used in C
    private boolean isResultsetClosed;      // result set status in current connection
    private int affectedRows = -1;

    static {
        final String name = "TD_LIBRARY_PATH";
        final String taosLibName = "taos";
        final String fn = System.mapLibraryName(taosLibName);
        final String separator = System.getProperty("path.separator");
        String lp = System.getProperty(name); // system property takes precedence over env
        if (lp == null) {
          lp = System.getenv(name);
        }
        try {
          if (lp == null) {
            // let system to choose how to search and load
            System.loadLibrary(taosLibName);
          } else {
            // we'll traverse the paths and try to load one after another until one succeeds
            final String[] paths = lp.split(separator);

            boolean found = false;
            for (final String path : paths) {
              final String p  = path + "/" + fn;
              try {
                System.load(p);
                found = true;
              } catch (UnsatisfiedLinkError e) {
              } catch (Exception e) {
              }
              if (found) break;
            }
            if (!found) {
              final String pattern = "dynamic libraries `{0}` not found in: `{1}`";
              final String msg = MessageFormat.format(pattern, fn, lp);
              throw new RuntimeException(msg);
            }
          }
        } catch (UnsatisfiedLinkError e) {
          final String pattern = "You can set the `{0}` through environment variables or " +
                           "Java system properties to specify the search path for dynamic libraries `{1}`";
          final String msg = MessageFormat.format(pattern, name, fn);
          throw new RuntimeException(msg, e);
        } catch (Exception e) {
          final String pattern = "You can set the `{0}` through environment variables or " +
                           "Java system properties to specify the search path for dynamic libraries `{1}`";
          final String msg = MessageFormat.format(pattern, name, fn);
          throw new RuntimeException(msg, e);
        }
    }

    /***********************************************************************/
    //NOTE: JDBC
    public static void init(Properties props) throws SQLWarning {
        synchronized (LOCK) {
            if (!isInitialized) {

                ObjectMapper objectMapper = JsonUtil.getObjectMapper();
                ObjectNode configJSON = objectMapper.createObjectNode();

                for (String key : props.stringPropertyNames()) {
                    configJSON.put(key, props.getProperty(key));
                }

                String cfg;

                try {
                    cfg = objectMapper.writeValueAsString(configJSON);
                } catch (JsonProcessingException e) {
                    throw TSDBError.createSQLWarning("Failed to parse properties to JSON string.");
                }

               setConfigImp(cfg);

                initImp(props.getProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, null));

                String locale = props.getProperty(TSDBDriver.PROPERTY_KEY_LOCALE);
                if (setOptions(0, locale) < 0) {
                    throw TSDBError.createSQLWarning("Failed to set locale: " + locale + ". System default will be used.");
                }
                String charset = props.getProperty(TSDBDriver.PROPERTY_KEY_CHARSET);
                if (setOptions(1, charset) < 0) {
                    throw TSDBError.createSQLWarning("Failed to set charset: " + charset + ". System default will be used.");
                }

                String timezone = props.getProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE);
                if (setOptions(2, timezone) < 0) {
                    throw TSDBError.createSQLWarning("Failed to set timezone: " + timezone + ". System default will be used.");
                }

                try{
                    handleTimeZone(timezone.trim());
                } catch (Exception e){
                }
                isInitialized = true;
                TaosGlobalConfig.setCharset(getTsCharset());
            }
        }
    }


    private static void handleTimeZone(String posixTimeZoneStr){
        //争取向前兼容POSIX 接口规范，只处理能处理的情况
        // 1. UTC 和 GMT，只处理小时情况，加号变减号，减号变加号
        if (posixTimeZoneStr.startsWith("UTC") || posixTimeZoneStr.startsWith("GMT")){
            if (posixTimeZoneStr.length() == 3){
                //标准时间
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("GMT")));
                return;
            }

            // 只处理UTC-8，UTC+8, GMT+8, GMT-8这样的格式
            String regex = "^(UTC|GMT)([+-])(\\d+)$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(posixTimeZoneStr);
            if (matcher.matches()) {
                String op = matcher.group(2);
                String hourStr = matcher.group(3);
                if (op.equals("+")){
                    op = "-";
                }else{
                    op = "+";
                }

                //只处理java可以处理的情况
                int hour = Integer.parseInt(hourStr);
                if (hour > 18){
                    return;
                }

                String timezone = String.format("GMT%s%02d:00", op, hour);
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(timezone)));
                return;
            }
            // 不支持的格式，不处理
        }

        // 2. 尝试处理Asia/Shanghai这种情况
        if (posixTimeZoneStr.contains("/")){
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(posixTimeZoneStr)));
        }
    }
    private static native void initImp(String configDir);

    private static native int setOptions(int optionIndex, String optionValue);

    private static native String getTsCharset();

    private static native TSDBException setConfigImp(String config);

    public boolean connect(String host, int port, String dbName, String user, String password) throws SQLException {
        if (this.taos != TSDBConstants.JNI_NULL_POINTER) {
            closeConnection();
            this.taos = TSDBConstants.JNI_NULL_POINTER;
        }

        if (!StringUtils.isEmpty(host) && host.contains("[")) {
            host = host.replace("[", "").replace("]", "");
        }

        this.taos = this.connectImp(host, port, dbName, user, password);
        if (this.taos == TSDBConstants.JNI_NULL_POINTER) {
            String errMsg = this.getErrMsg(0);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL, errMsg);
        }
        // invoke connectImp only here
        taosInfo.connOpenIncrement();
        return true;
    }

    private native long connectImp(String host, int port, String dbName, String user, String password);

    /**
     * Execute DML/DDL operation
     */
    public long executeQuery(String sql) throws SQLException {
        return this.executeQuery(sql, null);
    }

    private native long executeQueryImp(byte[] sqlBytes, long connection);

    public long executeQuery(String sql, Long reqId) throws SQLException {
        long pSql = 0L;
        try {
            if (null == reqId)
                pSql = this.executeQueryImp(sql.getBytes(TaosGlobalConfig.getCharset()), this.taos);
            else
                pSql = this.executeQueryWithReqId(sql.getBytes(TaosGlobalConfig.getCharset()), this.taos, reqId);
            taosInfo.stmtCountIncrement();
        } catch (UnsupportedEncodingException e) {
            this.freeResultSetImp(this.taos, pSql);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_ENCODING);
        }
        if (pSql == TSDBConstants.JNI_CONNECTION_NULL) {
            this.freeResultSetImp(this.taos, pSql);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        }
        if (pSql == TSDBConstants.JNI_SQL_NULL) {
            this.freeResultSetImp(this.taos, pSql);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_SQL_NULL);
        }
        if (pSql == TSDBConstants.JNI_OUT_OF_MEMORY) {
            this.freeResultSetImp(this.taos, pSql);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY);
        }

        int code = this.getErrCode(pSql);
        if (code != TSDBConstants.JNI_SUCCESS) {
            affectedRows = -1;
            String msg = this.getErrMsg(pSql);
            this.freeResultSetImp(this.taos, pSql);
            throw TSDBError.createSQLException(code, msg);
        }

        // Try retrieving result set for the executed SQL using the current connection pointer.
        pSql = this.getResultSetImp(this.taos, pSql);
        // if pSql == 0L that means resultset is closed
        isResultsetClosed = (pSql == TSDBConstants.JNI_NULL_POINTER);

        return pSql;
    }

    private native long executeQueryWithReqId(byte[] sqlBytes, long connection, long reqId);

    /**
     * Get recent error code by connection
     */
    public int getErrCode(long pSql) {
        return this.getErrCodeImp(this.taos, pSql);
    }

    private native int getErrCodeImp(long connection, long pSql);

    /**
     * Get recent error message by connection
     */
    public String getErrMsg(long pSql) {
        return this.getErrMsgImp(pSql);
    }

    private native String getErrMsgImp(long pSql);

    private native long getResultSetImp(long connection, long pSql);

    public boolean isUpdateQuery(long pSql) {
        return isUpdateQueryImp(this.taos, pSql) == 1;
    }

    private native long isUpdateQueryImp(long connection, long pSql);

    public boolean isClosed() {
        return this.taos == TSDBConstants.JNI_NULL_POINTER;
    }

    public boolean isResultsetClosed() {
        return this.isResultsetClosed;
    }

    /**
     * Free result set operation from C to release result set pointer by JNI
     */
    public int freeResultSet(long pSql) {
        int res = this.freeResultSetImp(this.taos, pSql);
        isResultsetClosed = true;
        return res;
    }

    private native int freeResultSetImp(long connection, long result);

    /**
     * Get affected rows count
     */
    public int getAffectedRows(long pSql) {
        int affectedRows = this.affectedRows;
        if (affectedRows < 0) {
            affectedRows = this.getAffectedRowsImp(this.taos, pSql);
        }
        return affectedRows;
    }

    private native int getAffectedRowsImp(long connection, long pSql);

    /**
     * Get schema metadata
     */
    public int getSchemaMetaData(long resultSet, List<ColumnMetaData> columnMetaData) {
        int ret = this.getSchemaMetaDataImp(this.taos, resultSet, columnMetaData);
        columnMetaData.forEach(column -> column.setColIndex(column.getColIndex() + 1));
        return ret;
    }

    private native int getSchemaMetaDataImp(long connection, long resultSet, List<ColumnMetaData> columnMetaData);

    /**
     * Get one row data
     */
    public int fetchRow(long resultSet, TSDBResultSetRowData rowData) {
        return this.fetchRowImp(this.taos, resultSet, rowData);
    }

    private native int fetchRowImp(long connection, long resultSet, TSDBResultSetRowData rowData);

    public int fetchBlock(long resultSet, TSDBResultSetBlockData blockData) {
        return this.fetchBlockImp(this.taos, resultSet, blockData);
    }

    private native int fetchBlockImp(long connection, long resultSet, TSDBResultSetBlockData blockData);

    /**
     * Get Result Time Precision.
     *
     * @return 0: ms, 1: us, 2: ns
     */
    public int getResultTimePrecision(long sqlObj) {
        return this.getResultTimePrecisionImp(this.taos, sqlObj);
    }

    private native int getResultTimePrecisionImp(long connection, long result);

    /**
     * Execute close operation from C to release connection pointer by JNI
     */
    public void closeConnection() throws SQLException {
        int code = this.closeConnectionImp(this.taos);

        if (code < 0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        } else if (code == 0) {
            this.taos = TSDBConstants.JNI_NULL_POINTER;
        } else {
            throw new SQLException("Undefined error code returned by TDengine when closing a connection");
        }

        // invoke closeConnectionImpl only here
        taosInfo.connectCloseIncrement();
    }

    private native int closeConnectionImp(long connection);

    /******************************************************************************************************/
    // NOTE: parameter binding
    public long prepareStmt(String sql) throws SQLException {
        return this.prepareStmt(sql, null);
    }

    private native long prepareStmtImp(byte[] sql, long con);

    public long prepareStmt(String sql, Long reqId) throws SQLException {
        long stmt;
        if (null == reqId)
            // jni error code can not return to java
            stmt = prepareStmtImp(sql.getBytes(), this.taos);
        else
            stmt = prepareStmtWithReqId(sql.getBytes(), this.taos, reqId);

        if (stmt == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL, "connection already closed");
        }
        if (stmt == TSDBConstants.JNI_SQL_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_SQL_NULL);
        }
        if (stmt == TSDBConstants.JNI_OUT_OF_MEMORY) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY);
        }
        if (stmt == TSDBConstants.JNI_TDENGINE_ERROR) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_TDENGINE_ERROR);
        }

        return stmt;
    }

    private native long prepareStmtWithReqId(byte[] sql, long con, long reqId);

    public void setBindTableName(long stmt, String tableName) throws SQLException {
        int code = setBindTableNameImp(stmt, tableName, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(code, "failed to set table name, reason: " + stmtErrorMsgImp(stmt, this.taos));
        }
    }

    private native int setBindTableNameImp(long stmt, String name, long conn);

    public void setBindTableNameAndTags(long stmt, String tableName, int numOfTags, ByteBuffer tags, ByteBuffer typeList, ByteBuffer lengthList, ByteBuffer nullList) throws SQLException {
        int code = setTableNameTagsImp(stmt, tableName, numOfTags, tags.array(), typeList.array(), lengthList.array(), nullList.array(), this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(code, "failed to bind table name and corresponding tags, reason: " + stmtErrorMsgImp(stmt, this.taos));
        }
    }

    private native int setTableNameTagsImp(long stmt, String name, int numOfTags, byte[] tags, byte[] typeList, byte[] lengthList, byte[] nullList, long conn);

    public void bindColumnDataArray(long stmt, ByteBuffer colDataList, ByteBuffer lengthList, ByteBuffer isNullList, int type, int bytes, int numOfRows, int columnIndex) throws SQLException {
        int code = bindColDataImp(stmt, colDataList.array(), lengthList.array(), isNullList.array(), type, bytes, numOfRows, columnIndex, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(code, "failed to bind column data, reason: " + stmtErrorMsgImp(stmt, this.taos));
        }
    }

    private native int bindColDataImp(long stmt, byte[] colDataList, byte[] lengthList, byte[] isNullList, int type, int bytes, int numOfRows, int columnIndex, long conn);

    public void executeBatch(long stmt) throws SQLException {
        int code = executeBatchImp(stmt, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(code, "failed to execute batch bind, reason: " + stmtErrorMsgImp(stmt, this.taos));
        }
    }

    public void addBatch(long stmt) throws SQLException {
        int code = addBatchImp(stmt, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(code, stmtErrorMsgImp(stmt, this.taos));
        }
    }

    private native int addBatchImp(long stmt, long con);

    private native int executeBatchImp(long stmt, long con);

    public void closeBatch(long stmt) throws SQLException {
        int code = closeStmt(stmt, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(code, "failed to close batch bind: " + stmtErrorMsgImp(stmt, this.taos));
        }
    }

    private native int closeStmt(long stmt, long con);

    private native String stmtErrorMsgImp(long stmt, long con);

    /*************************************************************************************************/
    // NOTE: schemaless-lines
    public void insertLines(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        long pSql = schemalessInsertImp(lines, this.taos, protocolType.ordinal(), timestampType.ordinal());
        releaseSchemalessInsert(pSql);
    }

    private void releaseSchemalessInsert(long pSql) throws SQLException {
        try {
            if (pSql == TSDBConstants.JNI_CONNECTION_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
            }
            if (pSql == TSDBConstants.JNI_OUT_OF_MEMORY) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY);
            }

            int code = this.getErrCode(pSql);
            if (code != TSDBConstants.JNI_SUCCESS) {
                String msg = this.getErrMsg(pSql);
                throw TSDBError.createSQLException(code, msg);
            }
        } finally {
            this.freeResultSetImp(this.taos, pSql);
        }
    }

    //    DLL_EXPORT TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision); // NOSONAR
    private native long schemalessInsertImp(String[] lines, long conn, int type, int precision);

    public void insertLinesWithReqId(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, long reqId) throws SQLException {
        long pSql = schemalessInsertWithReqId(this.taos, lines, protocolType.ordinal(), timestampType.ordinal(), reqId);
        releaseSchemalessInsert(pSql);
    }

    private native long schemalessInsertWithReqId(long conn, String[] lines, int type, int precision, long reqId);

    public void insertLinesWithTtl(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, int ttl) throws SQLException {
        long pSql = schemalessInsertWithTtl(this.taos, lines, protocolType.ordinal(), timestampType.ordinal(), ttl);
        releaseSchemalessInsert(pSql);
    }

    private native long schemalessInsertWithTtl(long conn, String[] lines, int type, int precision, int ttl);

    public void insertLinesWithTtlAndReqId(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, int ttl, long reqId) throws SQLException {
        long pSql = schemalessInsertWithTtlAndReqId(this.taos, lines, protocolType.ordinal(), timestampType.ordinal(), ttl, reqId);
        releaseSchemalessInsert(pSql);
    }

    //    DLL_EXPORT TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, // NOSONAR
    //                                                               int precision, int32_t ttl, int64_t reqid); // NOSONAR
    private native long schemalessInsertWithTtlAndReqId(long conn, String[] lines, int type, int precision, int ttl, long reqId);

    public int insertRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        if (null == line)
            throw TSDBError.createSQLException(ERROR_INVALID_VARIABLE);

        SchemalessResp resp = schemalessInsertRaw(this.taos, line, protocolType.ordinal(), timestampType.ordinal());
        if (TSDBConstants.JNI_SUCCESS != resp.getCode())
            throw TSDBError.createSQLException(resp.getCode(), resp.getMsg());
        return resp.getTotalRows();
    }

    private native SchemalessResp schemalessInsertRaw(long conn, String line, int type, int precision);

    public int insertRawWithReqId(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, long reqId) throws SQLException {
        if (null == line)
            throw TSDBError.createSQLException(ERROR_INVALID_VARIABLE);

        SchemalessResp resp = schemalessInsertRawWithReqId(this.taos, line, protocolType.ordinal(), timestampType.ordinal(), reqId);
        if (TSDBConstants.JNI_SUCCESS != resp.getCode())
            throw TSDBError.createSQLException(resp.getCode(), resp.getMsg());
        return resp.getTotalRows();
    }

    private native SchemalessResp schemalessInsertRawWithReqId(long conn, String line, int type, int precision, long reqId);


    public int insertRawWithTtl(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, int ttl) throws SQLException {
        if (null == line)
            throw TSDBError.createSQLException(ERROR_INVALID_VARIABLE);

        SchemalessResp resp = schemalessInsertRawWithTtl(this.taos, line, protocolType.ordinal(), timestampType.ordinal(), ttl);
        if (TSDBConstants.JNI_SUCCESS != resp.getCode())
            throw TSDBError.createSQLException(resp.getCode(), resp.getMsg());
        return resp.getTotalRows();
    }

    private native SchemalessResp schemalessInsertRawWithTtl(long conn, String line, int type, int precision, int ttl);

    public int insertRawWithTtlAndReqId(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, int ttl, long reqId) throws SQLException {
        if (null == line)
            throw TSDBError.createSQLException(ERROR_INVALID_VARIABLE);

        SchemalessResp resp = schemalessInsertRawWithTtlAndReqId(this.taos, line, protocolType.ordinal(), timestampType.ordinal(), ttl, reqId);
        if (TSDBConstants.JNI_SUCCESS != resp.getCode())
            throw TSDBError.createSQLException(resp.getCode(), resp.getMsg());
        return resp.getTotalRows();
    }

    //    DLL_EXPORT TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, // NOSONAR
    //                                                                   int protocol, int precision, int32_t ttl, int64_t reqid); // NOSONAR
    private native SchemalessResp schemalessInsertRawWithTtlAndReqId(long conn, String line, int type, int precision, int ttl, long reqId);

    /******************** VGroupID ************************/

    public int getTableVGroupID(String db, String table) throws SQLException {
        VGroupIDResp resp = getTableVgID(this.taos, db, table, new VGroupIDResp());
        if (resp.getCode() != 0) {
            throw TSDBError.createSQLException(resp.getCode(), "get table vGroup id fail");
        }
        return resp.getVgID();
    }

    private native VGroupIDResp getTableVgID(long conn, String db, String table, VGroupIDResp resp);
}
