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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * JNI connector
 */
public class TSDBJNIConnector {
    private static volatile Boolean isInitialized = false;

    private TaosInfo taosInfo = TaosInfo.getInstance();

    static {
        System.loadLibrary("taos");
        System.out.println("java.library.path:" + System.getProperty("java.library.path"));
    }

    /**
     * Connection pointer used in C
     */
    private long taos = TSDBConstants.JNI_NULL_POINTER;

    /**
     * Result set pointer for the current connection
     */
//    private long taosResultSetPointer = TSDBConstants.JNI_NULL_POINTER;

    /**
     * result set status in current connection
     */
    private boolean isResultsetClosed = true;
    private int affectedRows = -1;

    /**
     * Whether the connection is closed
     */
    public boolean isClosed() {
        return this.taos == TSDBConstants.JNI_NULL_POINTER;
    }

    /**
     * Returns the status of last result set in current connection
     */
    public boolean isResultsetClosed() {
        return this.isResultsetClosed;
    }

    /**
     * Initialize static variables in JNI to optimize performance
     */
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

    /**
     * Get connection pointer
     *
     * @throws SQLException
     */
    public boolean connect(String host, int port, String dbName, String user, String password) throws SQLException {
        if (this.taos != TSDBConstants.JNI_NULL_POINTER) {
//            this.closeConnectionImp(this.taos);
            closeConnection();
            this.taos = TSDBConstants.JNI_NULL_POINTER;
        }

        this.taos = this.connectImp(host, port, dbName, user, password);
        if (this.taos == TSDBConstants.JNI_NULL_POINTER) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        }
        // invoke connectImp only here
        taosInfo.conn_open_increment();
        return true;
    }

    private native long connectImp(String host, int port, String dbName, String user, String password);

    /**
     * Execute DML/DDL operation
     *
     * @throws SQLException
     */
    public long executeQuery(String sql) throws SQLException {
        // close previous result set if the user forgets to invoke the
        // free method to close previous result set.
//        if (!this.isResultsetClosed) {
//            freeResultSet(taosResultSetPointer);
//        }

        Long pSql = 0l;
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

    /**
     * Get resultset pointer
     * Each connection should have a single open result set at a time
     */
//    public long getResultSet() {
//        return taosResultSetPointer;
//    }
    private native long getResultSetImp(long connection, long pSql);

    public boolean isUpdateQuery(long pSql) {
        return isUpdateQueryImp(this.taos, pSql) == 1 ? true : false;
    }

    private native long isUpdateQueryImp(long connection, long pSql);

    /**
     * Free resultset operation from C to release resultset pointer by JNI
     */
    public int freeResultSet(long pSql) {
        int res = TSDBConstants.JNI_SUCCESS;
//        if (result != taosResultSetPointer && taosResultSetPointer != TSDBConstants.JNI_NULL_POINTER) {
//            throw new RuntimeException("Invalid result set pointer");
//        }

//        if (taosResultSetPointer != TSDBConstants.JNI_NULL_POINTER) {
        res = this.freeResultSetImp(this.taos, pSql);
//            taosResultSetPointer = TSDBConstants.JNI_NULL_POINTER;
//        }

        isResultsetClosed = true;
        return res;
    }

    /**
     * Close the open result set which is associated to the current connection. If the result set is already
     * closed, return 0 for success.
     */
//    public int freeResultSet() {
//        int resCode = TSDBConstants.JNI_SUCCESS;
//        if (!isResultsetClosed) {
//            resCode = this.freeResultSetImp(this.taos, this.taosResultSetPointer);
//            taosResultSetPointer = TSDBConstants.JNI_NULL_POINTER;
//            isResultsetClosed = true;
//        }
//        return resCode;
//    }
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
        return this.getSchemaMetaDataImp(this.taos, resultSet, columnMetaData);
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
     * Execute close operation from C to release connection pointer by JNI
     *
     * @throws SQLException
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
     *
     * @param subscription
     */
    void unsubscribe(long subscription, boolean isKeep) {
        unsubscribeImp(subscription, isKeep);
    }

    private native void unsubscribeImp(long subscription, boolean isKeep);

    /**
     * Validate if a <I>create table</I> sql statement is correct without actually creating that table
     */
    public boolean validateCreateTableSql(String sql) {
        long connection = taos;
        int res = validateCreateTableSqlImp(connection, sql.getBytes());
        return res != 0 ? false : true;
    }

    private native int validateCreateTableSqlImp(long connection, byte[] sqlBytes);
}
