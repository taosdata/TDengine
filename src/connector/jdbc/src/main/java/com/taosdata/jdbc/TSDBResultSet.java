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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class TSDBResultSet implements ResultSet {
	private TSDBJNIConnector jniConnector = null;

	private long resultSetPointer = 0L;
	private List<ColumnMetaData> columnMetaDataList = new ArrayList<ColumnMetaData>();

	private TSDBResultSetRowData rowData;
	private TSDBResultSetBlockData blockData;

	private boolean batchFetch = false;
	private boolean lastWasNull = false;
	private final int COLUMN_INDEX_START_VALUE = 1;

	private int rowIndex = 0;

	public TSDBJNIConnector getJniConnector() {
		return jniConnector;
	}

	public void setJniConnector(TSDBJNIConnector jniConnector) {
		this.jniConnector = jniConnector;
	}

	public long getResultSetPointer() {
		return resultSetPointer;
	}

	public void setResultSetPointer(long resultSetPointer) {
		this.resultSetPointer = resultSetPointer;
	}

	public void setBatchFetch(boolean batchFetch) {
		this.batchFetch = batchFetch;
	}
	
	public Boolean getBatchFetch() {
		return this.batchFetch;
	}

	public List<ColumnMetaData> getColumnMetaDataList() {
		return columnMetaDataList;
	}

	public void setColumnMetaDataList(List<ColumnMetaData> columnMetaDataList) {
		this.columnMetaDataList = columnMetaDataList;
	}

	public TSDBResultSetRowData getRowData() {
		return rowData;
	}

	public void setRowData(TSDBResultSetRowData rowData) {
		this.rowData = rowData;
	}

	public boolean isLastWasNull() {
		return lastWasNull;
	}

	public void setLastWasNull(boolean lastWasNull) {
		this.lastWasNull = lastWasNull;
	}

	public TSDBResultSet() {
	}

	public TSDBResultSet(TSDBJNIConnector connector, long resultSetPointer) throws SQLException {
		this.jniConnector = connector;
		this.resultSetPointer = resultSetPointer;
		int code = this.jniConnector.getSchemaMetaData(this.resultSetPointer, this.columnMetaDataList);
		if (code == TSDBConstants.JNI_CONNECTION_NULL) {
			throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
		} else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
			throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_RESULT_SET_NULL));
		} else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
			throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_NUM_OF_FIELDS_0));
		}

		this.rowData = new TSDBResultSetRowData(this.columnMetaDataList.size());
		this.blockData = new TSDBResultSetBlockData(this.columnMetaDataList, this.columnMetaDataList.size());
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean next() throws SQLException {
		if (this.getBatchFetch()) {
			if (this.blockData.forward()) {
				return true;
			}

			int code = this.jniConnector.fetchBlock(this.resultSetPointer, this.blockData);
			this.blockData.reset();

			if (code == TSDBConstants.JNI_CONNECTION_NULL) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
			} else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_RESULT_SET_NULL));
			} else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_NUM_OF_FIELDS_0));
			} else if (code == TSDBConstants.JNI_FETCH_END) {
				return false;
			}

			return true;
		} else {
			if (rowData != null) {
				this.rowData.clear();
			}

			int code = this.jniConnector.fetchRow(this.resultSetPointer, this.rowData);
			if (code == TSDBConstants.JNI_CONNECTION_NULL) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
			} else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_RESULT_SET_NULL));
			} else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_NUM_OF_FIELDS_0));
			} else if (code == TSDBConstants.JNI_FETCH_END) {
				return false;
			} else {
				return true;
			}
		}
	}

	public void close() throws SQLException {
		if (this.jniConnector != null) {
			int code = this.jniConnector.freeResultSet(this.resultSetPointer);
			if (code == TSDBConstants.JNI_CONNECTION_NULL) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
			} else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
				throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_RESULT_SET_NULL));
			}
		}
	}

	public boolean wasNull() throws SQLException {
		return this.lastWasNull;
	}

	public String getString(int columnIndex) throws SQLException {
		String res = null;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = this.rowData.getString(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
			return res;
		} else {
			return this.blockData.getString(colIndex);
		}
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		boolean res = false;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = this.rowData.getBoolean(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
		} else {
			return this.blockData.getBoolean(colIndex);
		}

		return res;
	}

	public byte getByte(int columnIndex) throws SQLException {
		byte res = 0;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = (byte) this.rowData.getInt(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
			return res;
		} else {
			return (byte) this.blockData.getInt(colIndex);
		}
	}

	public short getShort(int columnIndex) throws SQLException {
		short res = 0;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = (short) this.rowData.getInt(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
			return res;
		} else {
			return (short) this.blockData.getInt(colIndex);
		}
	}

	public int getInt(int columnIndex) throws SQLException {
		int res = 0;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = this.rowData.getInt(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
			return res;
		} else {
			return this.blockData.getInt(colIndex);
		}

	}

	public long getLong(int columnIndex) throws SQLException {
		long res = 0l;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = this.rowData.getLong(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
			return res;
		} else {
			return this.blockData.getLong(colIndex);
		}
	}

	public float getFloat(int columnIndex) throws SQLException {
		float res = 0;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = this.rowData.getFloat(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
			return res;
		} else {
			return (float) this.blockData.getDouble(colIndex);
		}
	}

	public double getDouble(int columnIndex) throws SQLException {
		double res = 0;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = this.rowData.getDouble(colIndex, this.columnMetaDataList.get(colIndex).getColType());
			}
			return res;
		} else {
			return this.blockData.getDouble(colIndex);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.sql.ResultSet#getBigDecimal(int, int)
	 *
	 * @deprecated Use {@code getBigDecimal(int columnIndex)} or {@code
	 * getBigDecimal(String columnLabel)}
	 */
	@Deprecated
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return new BigDecimal(getLong(columnIndex));
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return getString(columnIndex).getBytes();
	}

	public Date getDate(int columnIndex) throws SQLException {
		int colIndex = getTrueColumnIndex(columnIndex);
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Time getTime(int columnIndex) throws SQLException {
		int colIndex = getTrueColumnIndex(columnIndex);
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Timestamp res = null;
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			if (!lastWasNull) {
				res = this.rowData.getTimestamp(colIndex);
			}
			return res;
		} else {
			return this.blockData.getTimestamp(columnIndex);
		}
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.sql.ResultSet#getUnicodeStream(int)
	 *
	 * * @deprecated use <code>getCharacterStream</code> in place of
	 * <code>getUnicodeStream</code>
	 */
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public String getString(String columnLabel) throws SQLException {
		return this.getString(this.findColumn(columnLabel));
	}

	public boolean getBoolean(String columnLabel) throws SQLException {
		return this.getBoolean(this.findColumn(columnLabel));
	}

	public byte getByte(String columnLabel) throws SQLException {
		return this.getByte(this.findColumn(columnLabel));
	}

	public short getShort(String columnLabel) throws SQLException {
		return this.getShort(this.findColumn(columnLabel));
	}

	public int getInt(String columnLabel) throws SQLException {
		return this.getInt(this.findColumn(columnLabel));
	}

	public long getLong(String columnLabel) throws SQLException {
		return this.getLong(this.findColumn(columnLabel));
	}

	public float getFloat(String columnLabel) throws SQLException {
		return this.getFloat(this.findColumn(columnLabel));
	}

	public double getDouble(String columnLabel) throws SQLException {
		return this.getDouble(this.findColumn(columnLabel));
	}

	/*
	 * used by spark
	 */
	@Deprecated
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return this.getBigDecimal(this.findColumn(columnLabel), scale);
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		return this.getBytes(this.findColumn(columnLabel));
	}

	public Date getDate(String columnLabel) throws SQLException {
		return this.getDate(this.findColumn(columnLabel));
	}

	public Time getTime(String columnLabel) throws SQLException {
		return this.getTime(this.findColumn(columnLabel));
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return this.getTimestamp(this.findColumn(columnLabel));
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Deprecated
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void clearWarnings() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public String getCursorName() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return new TSDBResultSetMetaData(this.columnMetaDataList);
	}

	public Object getObject(int columnIndex) throws SQLException {
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			return this.rowData.get(colIndex);
		} else {
			return this.blockData.get(colIndex);
		}
	}

	public Object getObject(String columnLabel) throws SQLException {
		return this.getObject(this.findColumn(columnLabel));
	}

	public int findColumn(String columnLabel) throws SQLException {
		Iterator<ColumnMetaData> colMetaDataIt = this.columnMetaDataList.iterator();
		while (colMetaDataIt.hasNext()) {
			ColumnMetaData colMetaData = colMetaDataIt.next();
			if (colMetaData.getColName() != null && colMetaData.getColName().equalsIgnoreCase(columnLabel)) {
				return colMetaData.getColIndex() + 1;
			}
		}
		throw new SQLException(TSDBConstants.INVALID_VARIABLES);
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	/*
	 * used by spark
	 */
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		int colIndex = getTrueColumnIndex(columnIndex);

		if (!this.getBatchFetch()) {
			this.lastWasNull = this.rowData.wasNull(colIndex);
			return new BigDecimal(this.rowData.getLong(colIndex, this.columnMetaDataList.get(colIndex).getColType()));
		} else {
			return new BigDecimal(this.blockData.getLong(colIndex));
		}
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return this.getBigDecimal(this.findColumn(columnLabel));
	}

	public boolean isBeforeFirst() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean isAfterLast() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean isFirst() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean isLast() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void beforeFirst() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void afterLast() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean first() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean last() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public int getRow() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean absolute(int row) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean relative(int rows) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean previous() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public int getFetchDirection() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void setFetchSize(int rows) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public int getFetchSize() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	public int getConcurrency() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean rowUpdated() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean rowInserted() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean rowDeleted() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void insertRow() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateRow() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void deleteRow() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void refreshRow() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void cancelRowUpdates() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void moveToInsertRow() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void moveToCurrentRow() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Statement getStatement() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public int getHoldability() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public boolean isClosed() throws SQLException {
		boolean isClosed = true;
		if (jniConnector != null) {
			isClosed = jniConnector.isResultsetClosed();
		}
		return isClosed;
	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public String getNString(int columnIndex) throws SQLException {
		int colIndex = getTrueColumnIndex(columnIndex);
		return (String) rowData.get(colIndex);
	}

	public String getNString(String columnLabel) throws SQLException {
		return (String) this.getString(columnLabel);
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	private int getTrueColumnIndex(int columnIndex) throws SQLException {
		if (columnIndex < this.COLUMN_INDEX_START_VALUE) {
			throw new SQLException("Column Index out of range, " + columnIndex + " < " + this.COLUMN_INDEX_START_VALUE);
		}

		int numOfCols = this.columnMetaDataList.size();
		if (columnIndex > numOfCols) {
			throw new SQLException("Column Index out of range, " + columnIndex + " > " + numOfCols);
		}

		return columnIndex - 1;
	}
}
