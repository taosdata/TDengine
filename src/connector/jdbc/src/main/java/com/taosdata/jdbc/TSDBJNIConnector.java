package com.taosdata.jdbc;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.TaosInfo;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Properties;

/**
 * JNI connector
 */
public class TSDBJNIConnector {
    private static final Object LOCK = new Object();
    private static volatile boolean isInitialized;

    private final TaosInfo taosInfo = TaosInfo.getInstance();
    private long taos = TSDBConstants.JNI_NULL_POINTER;     // Connection pointer used in C
    private boolean isResultsetClosed;      // result set status in current connection
    private int affectedRows = -1;

    static {
        System.loadLibrary("taos");
    }

    public static void init(Properties props) throws SQLWarning {
        synchronized (LOCK) {
            if (!isInitialized) {

                JSONObject configJSON = new JSONObject();
                for (String key : props.stringPropertyNames()) {
                    configJSON.put(key, props.getProperty(key));
                }
                setConfigImp(configJSON.toJSONString());

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
                isInitialized = true;
                TaosGlobalConfig.setCharset(getTsCharset());
            }
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

        this.taos = this.connectImp(host, port, dbName, user, password);
        if (this.taos == TSDBConstants.JNI_NULL_POINTER) {
            String errMsg = this.getErrMsg(0);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL, errMsg);
        }
        // invoke connectImp only here
        taosInfo.conn_open_increment();
        return true;
    }

    private native long connectImp(String host, int port, String dbName, String user, String password);

    /**
     * Execute DML/DDL operation
     */
    public long executeQuery(String sql) throws SQLException {
        long pSql = 0L;
        try {
            pSql = this.executeQueryImp(sql.getBytes(TaosGlobalConfig.getCharset()), this.taos);
            taosInfo.stmt_count_increment();
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

    private native long executeQueryImp(byte[] sqlBytes, long connection);

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
        taosInfo.connect_close_increment();
    }

    private native int closeConnectionImp(long connection);

    /**
     * Create a subscription
     */
    long subscribe(String topic, String sql, boolean restart) {
        return subscribeImp(this.taos, restart, topic, sql, 0);
    }

    private native long subscribeImp(long connection, boolean restart, String topic, String sql, int period);

    /**
     * Consume a subscription
     */
    long consume(long subscription) {
        return this.consumeImp(subscription);
    }

    private native long consumeImp(long subscription);

    /**
     * Unsubscribe, close a subscription
     */
    void unsubscribe(long subscription, boolean isKeep) {
        unsubscribeImp(subscription, isKeep);
    }

    private native void unsubscribeImp(long subscription, boolean isKeep);

    public long prepareStmt(String sql) throws SQLException {
        long stmt = prepareStmtImp(sql.getBytes(), this.taos);

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

    private native long prepareStmtImp(byte[] sql, long con);

    public void setBindTableName(long stmt, String tableName) throws SQLException {
        int code = setBindTableNameImp(stmt, tableName, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "failed to set table name");
        }
    }

    private native int setBindTableNameImp(long stmt, String name, long conn);

    public void setBindTableNameAndTags(long stmt, String tableName, int numOfTags, ByteBuffer tags, ByteBuffer typeList, ByteBuffer lengthList, ByteBuffer nullList) throws SQLException {
        int code = setTableNameTagsImp(stmt, tableName, numOfTags, tags.array(), typeList.array(), lengthList.array(), nullList.array(), this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "failed to bind table name and corresponding tags");
        }
    }

    private native int setTableNameTagsImp(long stmt, String name, int numOfTags, byte[] tags, byte[] typeList, byte[] lengthList, byte[] nullList, long conn);

    public void bindColumnDataArray(long stmt, ByteBuffer colDataList, ByteBuffer lengthList, ByteBuffer isNullList, int type, int bytes, int numOfRows, int columnIndex) throws SQLException {
        int code = bindColDataImp(stmt, colDataList.array(), lengthList.array(), isNullList.array(), type, bytes, numOfRows, columnIndex, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "failed to bind column data");
        }
    }

    private native int bindColDataImp(long stmt, byte[] colDataList, byte[] lengthList, byte[] isNullList, int type, int bytes, int numOfRows, int columnIndex, long conn);

    public void executeBatch(long stmt) throws SQLException {
        int code = executeBatchImp(stmt, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "failed to execute batch bind");
        }
    }

    private native int executeBatchImp(long stmt, long con);

    public void closeBatch(long stmt) throws SQLException {
        int code = closeStmt(stmt, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "failed to close batch bind");
        }
    }

    private native int closeStmt(long stmt, long con);

    public void insertLines(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        int code = insertLinesImp(lines, this.taos, protocolType.ordinal(), timestampType.ordinal());
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "failed to insertLines");
        }
    }

    private native int insertLinesImp(String[] lines, long conn, int type, int precision);


}
