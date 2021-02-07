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
import java.sql.Date;
import java.sql.*;
import java.util.*;

/*
 * TDengine only supports a subset of the standard SQL, thus this implemetation of the
 * standard JDBC API contains more or less some adjustments customized for certain
 * compatibility needs.
 */
public class DatabaseMetaDataResultSet implements ResultSet {

    private List<ColumnMetaData> columnMetaDataList;
    private List<TSDBResultSetRowData> rowDataList;
    private TSDBResultSetRowData rowCursor;

    // position of cursor, starts from 0 as beforeFirst, increases as next() is called
    private int cursorRowNumber = 0;

    public DatabaseMetaDataResultSet() {
        rowDataList = new ArrayList<TSDBResultSetRowData>();
        columnMetaDataList = new ArrayList<ColumnMetaData>();
    }

    public List<TSDBResultSetRowData> getRowDataList() {
        return rowDataList;
    }

    public void setRowDataList(List<TSDBResultSetRowData> rowDataList) {
        this.rowDataList = rowDataList;
    }

    public List<ColumnMetaData> getColumnMetaDataList() {
        return columnMetaDataList;
    }

    public void setColumnMetaDataList(List<ColumnMetaData> columnMetaDataList) {
        this.columnMetaDataList = columnMetaDataList;
    }

    public TSDBResultSetRowData getRowCursor() {
        return rowCursor;
    }

    public void setRowCursor(TSDBResultSetRowData rowCursor) {
        this.rowCursor = rowCursor;
    }

    @Override
    public boolean next() throws SQLException {
//        boolean ret = false;
//        if (rowDataList.size() > 0) {
//            ret = rowDataList.iterator().hasNext();
//            if (ret) {
//                rowCursor = rowDataList.iterator().next();
//                cursorRowNumber++;
//            }
//        }
//        return ret;

        /**** add by zyyang 2020-09-29 ****************/
        boolean ret = false;
        if (!rowDataList.isEmpty() && cursorRowNumber < rowDataList.size()) {
            rowCursor = rowDataList.get(cursorRowNumber++);
            ret = true;
        }

        return ret;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        columnIndex--;
        int colType = columnMetaDataList.get(columnIndex).getColType();
        return rowCursor.getString(columnIndex, colType);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        columnIndex--;
        return rowCursor.getBoolean(columnIndex, columnMetaDataList.get(columnIndex).getColType());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        columnIndex--;
        return (byte) rowCursor.getInt(columnIndex, columnMetaDataList.get(columnIndex).getColType());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        columnIndex--;
        return (short) rowCursor.getInt(columnIndex, columnMetaDataList.get(columnIndex).getColType());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        columnIndex--;
        return rowCursor.getInt(columnIndex, columnMetaDataList.get(columnIndex).getColType());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        columnIndex--;
        return rowCursor.getLong(columnIndex, columnMetaDataList.get(columnIndex).getColType());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        columnIndex--;
        return rowCursor.getFloat(columnIndex, columnMetaDataList.get(columnIndex).getColType());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        columnIndex--;
        return rowCursor.getDouble(columnIndex, columnMetaDataList.get(columnIndex).getColType());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        columnIndex--;
        return new BigDecimal(rowCursor.getDouble(columnIndex, columnMetaDataList.get(columnIndex).getColType()));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        columnIndex--;
        return (rowCursor.getString(columnIndex, columnMetaDataList.get(columnIndex).getColType())).getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        columnIndex--;
        return rowCursor.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new TSDBResultSetMetaData(this.columnMetaDataList);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return rowCursor.get(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return rowCursor.get(findColumn(columnLabel));
    }

    @Override
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

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return new BigDecimal(rowCursor.getDouble(columnIndex, columnMetaDataList.get(columnIndex).getColType()));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return cursorRowNumber == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return rowDataList.iterator().hasNext();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return cursorRowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return cursorRowNumber == rowDataList.size();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public int getRow() throws SQLException {
        if (cursorRowNumber > 0 && cursorRowNumber <= rowDataList.size()) {
            return cursorRowNumber;
        } else {
            return 0;
        }

    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    private int getTrueColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw new SQLException("Column Index out of range, " + columnIndex + " < " + 1);
        }

        int numOfCols = this.columnMetaDataList.size();
        if (columnIndex > numOfCols) {
            throw new SQLException("Column Index out of range, " + columnIndex + " > " + numOfCols);
        }

        return columnIndex - 1;
    }
}
