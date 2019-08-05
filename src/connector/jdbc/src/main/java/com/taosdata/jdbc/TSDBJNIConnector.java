/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

public class TSDBJNIConnector {
	static volatile Boolean isInitialized = false;

	static {
		System.loadLibrary("taos");
	}

	/**
	 * Connection pointer used in C
	 */
	private long taos = TSDBConstants.JNI_NULL_POINTER;

	/**
	 * Result set pointer for the current connection
	 */
	private long taosResultSetPointer = TSDBConstants.JNI_NULL_POINTER;

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
	 * @return
	 */
	public boolean isResultsetClosed() {
		return this.isResultsetClosed;
	}
	
	/**
	 * Initialize static variables in JNI to optimize performance
	 */
	public static void init(String configDir, String locale, String charset, String timezone) throws SQLWarning {
		synchronized(isInitialized) {
			if (!isInitialized) {
				initImp(configDir);
				if (setOptions(0, locale) < 0) {
					throw new SQLWarning(TSDBConstants.WrapErrMsg("Failed to set locale: " + locale + ". System default will be used."));
				}
                if (setOptions(1, charset) < 0) {
                    throw new SQLWarning(TSDBConstants.WrapErrMsg("Failed to set charset: " + charset + ". System default will be used."));
                }
				if (setOptions(2, timezone) < 0) {
					throw new SQLWarning(TSDBConstants.WrapErrMsg("Failed to set timezone: " + timezone + ". System default will be used."));
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
			this.closeConnectionImp(this.taos);
			this.taos = TSDBConstants.JNI_NULL_POINTER;
		}

		this.taos = this.connectImp(host, port, dbName, user, password);
		if (this.taos == TSDBConstants.JNI_NULL_POINTER) {
			throw new SQLException(TSDBConstants.WrapErrMsg(this.getErrMsg()), "", this.getErrCode());
		}

		return true;
	}

	private native long connectImp(String host, int port, String dbName, String user, String password);

	/**
	 * Execute DML/DDL operation
	 * 
	 * @throws SQLException
	 */
	public int executeQuery(String sql) throws SQLException {
        if (!this.isResultsetClosed) {
        	freeResultSet(taosResultSetPointer);
        }
        
        int code;
        try {
            code = this.executeQueryImp(sql.getBytes(TaosGlobalConfig.getCharset()), this.taos);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException(TSDBConstants.WrapErrMsg("Unsupported encoding"));
        }
		affectedRows = code;
        if (code < 0) {
            affectedRows = -1;
            if (code == TSDBConstants.JNI_TDENGINE_ERROR) {
                throw new SQLException(TSDBConstants.WrapErrMsg(this.getErrMsg()), "", this.getErrCode());
            } else {
                throw new SQLException(TSDBConstants.FixErrMsg(code), "", this.getErrCode());
            }
        }

        // Try retrieving result set for the executed SQLusing the current connection pointer. If the executed
        // SQL is a DML/DDL which doesn't return a result set, then taosResultSetPointer should be 0L. Otherwise,
        // taosResultSetPointer should be a non-zero value.
        taosResultSetPointer = this.getResultSetImp(this.taos);
        if (taosResultSetPointer != TSDBConstants.JNI_NULL_POINTER) {
            isResultsetClosed = false;
        }
        return code;
	}

	private native int executeQueryImp(byte[] sqlBytes, long connection);

	/**
	 * Get recent error code by connection
	 */
	public int getErrCode() {
		return Math.abs(this.getErrCodeImp(this.taos));
	}

	private native int getErrCodeImp(long connection);

	/**
	 * Get recent error message by connection
	 */
	public String getErrMsg() {
		return this.getErrMsgImp(this.taos);
	}

	private native String getErrMsgImp(long connection);

	/**
	 * Get resultset pointer
     * Each connection should have a single open result set at a time
	 */
	public long getResultSet() {
        return taosResultSetPointer;
	}

	private native long getResultSetImp(long connection);

	/**
	 * Free resultset operation from C to release resultset pointer by JNI
	 */
	public int freeResultSet(long result) {
        int res = TSDBConstants.JNI_SUCCESS;
        if (result != taosResultSetPointer && taosResultSetPointer != TSDBConstants.JNI_NULL_POINTER) {
            throw new RuntimeException("Invalid result set pointer");
        } else if (taosResultSetPointer != TSDBConstants.JNI_NULL_POINTER){
            res = this.freeResultSetImp(this.taos, result);
            isResultsetClosed = true; // reset resultSetPointer to 0 after freeResultSetImp() return
            taosResultSetPointer = TSDBConstants.JNI_NULL_POINTER;
        }
        return res;
	}

    /**
     * Close the open result set which is associated to the current connection. If the result set is already
     * closed, return 0 for success.
     * @return
     */
	public int freeResultSet() {
	    int resCode = TSDBConstants.JNI_SUCCESS;
        if (!isResultsetClosed) {
            resCode = this.freeResultSetImp(this.taos, this.taosResultSetPointer);
            taosResultSetPointer = TSDBConstants.JNI_NULL_POINTER;
        }
        return resCode;
    }

	private native int freeResultSetImp(long connection, long result);

	/**
	 * Get affected rows count
	 */
	public int getAffectedRows() {
	    int affectedRows = this.affectedRows;
        if (affectedRows < 0) {
            affectedRows = this.getAffectedRowsImp(this.taos);
        }
        return affectedRows;
	}

	private native int getAffectedRowsImp(long connection);

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

	/**
	 * Execute close operation from C to release connection pointer by JNI
	 * 
	 * @throws SQLException
	 */
	public void closeConnection() throws SQLException {
		int code = this.closeConnectionImp(this.taos);
		if (code < 0) {
			throw new SQLException(TSDBConstants.FixErrMsg(code), "", this.getErrCode());
		} else if (code == 0){
			this.taos = TSDBConstants.JNI_NULL_POINTER;
		} else {
		    throw new SQLException("Undefined error code returned by TDengine when closing a connection");
        }
	}

	private native int closeConnectionImp(long connection);

	/**
	 * Subscribe to a table in TSDB
	 */
	public long subscribe(String host, String user, String password, String database, String table, long time, int period){
		return subscribeImp(host, user, password, database, table, time, period);
	}

	private native long subscribeImp(String host, String user, String password, String database, String table, long time, int period);

	/**
	 * Consume a subscribed table
	 */
	public TSDBResultSetRowData consume(long subscription) {
		return this.consumeImp(subscription);
	}

	private native TSDBResultSetRowData consumeImp(long subscription);

	/**
	 * Unsubscribe a table
	 * @param subscription
	 */
	public void unsubscribe(long subscription) {
		unsubscribeImp(subscription);
	}

	private native void unsubscribeImp(long subscription);

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
