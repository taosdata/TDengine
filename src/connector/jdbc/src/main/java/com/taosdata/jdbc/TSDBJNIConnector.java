/**
 * *************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * ***************************************************************************
 */
package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.TaosInfo;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * JNI connector
 */
public class TSDBJNIConnector {
    private static volatile Boolean isInitialized = false;

    private final TaosInfo taosInfo = TaosInfo.getInstance();
    private long taos = TSDBConstants.JNI_NULL_POINTER;     // Connection pointer used in C
    private boolean isResultsetClosed;      // result set status in current connection
    private int affectedRows = -1;

    static {
        System.loadLibrary("taos");
    }

    public boolean isClosed() {
        return this.taos == TSDBConstants.JNI_NULL_POINTER;
    }

    public boolean isResultsetClosed() {
        return this.isResultsetClosed;
    }

    public static void init(String configDir, String locale, String charset, String timezone) throws SQLWarning {
        synchronized (isInitialized) {
            if (!isInitialized) {
                initImp(configDir);
                if (setOptions(0, locale) < 0) {
                    throw TSDBError.createSQLWarning("Failed to set locale: " + locale + ". System default will be used.");
                }
                if (setOptions(1, charset) < 0) {
                    throw TSDBError.createSQLWarning("Failed to set charset: " + charset + ". System default will be used.");
                }
                if (setOptions(2, timezone) < 0) {
                    throw TSDBError.createSQLWarning("Failed to set timezone: " + timezone + ". System default will be used.");
                }
                isInitialized = true;
                TaosGlobalConfig.setCharset(getTsCharset());
            }
        }
    }

    public static native void initImp(String configDir);

    public static native int setOptions(int optionIndex, String optionValue);

    public static native String getTsCharset();

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
        } catch (Exception e) {
            e.printStackTrace();
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
    long subscribe(String topic, String sql, boolean restart, int period) {
        return subscribeImp(this.taos, restart, topic, sql, period);
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

    /**
     * Validate if a <I>create table</I> SQL statement is correct without actually creating that table
     */
    public boolean validateCreateTableSql(String sql) {
        int res = validateCreateTableSqlImp(taos, sql.getBytes());
        return res == 0;
    }

    private native int validateCreateTableSqlImp(long connection, byte[] sqlBytes);

    public long prepareStmt(String sql) throws SQLException {
        long stmt;
        try {
            stmt = prepareStmtImp(sql.getBytes(), this.taos);
        } catch (Exception e) {
            e.printStackTrace();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_ENCODING);
        }

        if (stmt == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        }

        if (stmt == TSDBConstants.JNI_SQL_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_SQL_NULL);
        }

        if (stmt == TSDBConstants.JNI_OUT_OF_MEMORY) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY);
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
        int code = setTableNameTagsImp(stmt, tableName, numOfTags, tags.array(), typeList.array(), lengthList.array(),
                nullList.array(), this.taos);
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

    public void insertLines(String[] lines) throws SQLException {
        int code = insertLinesImp(lines, this.taos);
        if (code != TSDBConstants.JNI_SUCCESS) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "failed to insertLines");
        }
    }

    private native int insertLinesImp(String[] lines, long conn);

    private native int setConfig(String config);

}
