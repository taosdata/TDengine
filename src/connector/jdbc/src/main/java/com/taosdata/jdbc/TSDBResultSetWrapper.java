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
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/*
 * TDengine only supports a subset of the standard SQL, thus this implemetation of the
 * standard JDBC API contains more or less some adjustments customized for certain
 * compatibility needs.
 */
public class TSDBResultSetWrapper implements ResultSet {

    private ResultSet originalResultSet;

    public ResultSet getOriginalResultSet() {
        return originalResultSet;
    }

    public void setOriginalResultSet(ResultSet originalResultSet) {
        this.originalResultSet = originalResultSet;
    }

    @Override
    public boolean next() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.next();
    }

    @Override
    public void close() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        this.originalResultSet.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getDouble(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBigDecimal(columnIndex, scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        if (columnIndex <= 1) {
            return this.originalResultSet.getBytes(columnIndex);
        } else {
            return null;
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBoolean(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getByte(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getDouble(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBigDecimal(columnLabel, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getTimestamp(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getAsciiStream(columnLabel);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBinaryStream(columnLabel);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        this.originalResultSet.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getMetaData();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getObject(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.findColumn(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getCharacterStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getBigDecimal(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        this.originalResultSet.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        this.originalResultSet.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.first();
    }

    @Override
    public boolean last() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.last();
    }

    @Override
    public int getRow() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.getRow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        if (originalResultSet == null) {
            throw new SQLException("No original result set is injected");
        }
        return this.originalResultSet.previous();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.originalResultSet.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.originalResultSet.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.originalResultSet.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.originalResultSet.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return this.originalResultSet.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return this.originalResultSet.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return this.originalResultSet.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return this.originalResultSet.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return this.originalResultSet.rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        this.originalResultSet.updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        this.originalResultSet.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        this.originalResultSet.updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        this.originalResultSet.updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        this.originalResultSet.updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        this.originalResultSet.updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        this.originalResultSet.updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        this.originalResultSet.updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        this.originalResultSet.updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        this.originalResultSet.updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        this.originalResultSet.updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        this.originalResultSet.updateDate(columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        this.originalResultSet.updateTime(columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        this.originalResultSet.updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        this.originalResultSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        this.originalResultSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        this.originalResultSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        this.originalResultSet.updateObject(columnIndex, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        this.originalResultSet.updateObject(columnIndex, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        this.originalResultSet.updateNull(columnLabel);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        this.originalResultSet.updateBoolean(columnLabel, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        this.originalResultSet.updateByte(columnLabel, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        this.originalResultSet.updateShort(columnLabel, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        this.originalResultSet.updateInt(columnLabel, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        this.originalResultSet.updateLong(columnLabel, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        this.originalResultSet.updateFloat(columnLabel, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        this.originalResultSet.updateDouble(columnLabel, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        this.originalResultSet.updateBigDecimal(columnLabel, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        this.originalResultSet.updateString(columnLabel, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        this.originalResultSet.updateBytes(columnLabel, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        this.originalResultSet.updateDate(columnLabel, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        this.originalResultSet.updateTime(columnLabel, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        this.originalResultSet.updateTimestamp(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        this.originalResultSet.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        this.originalResultSet.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        this.originalResultSet.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        this.originalResultSet.updateObject(columnLabel, x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        this.originalResultSet.updateObject(columnLabel, x);
    }

    @Override
    public void insertRow() throws SQLException {
        this.originalResultSet.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        this.originalResultSet.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        this.originalResultSet.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        this.originalResultSet.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        this.originalResultSet.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        this.originalResultSet.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        this.originalResultSet.moveToCurrentRow();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.originalResultSet.getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return this.originalResultSet.getObject(columnIndex, map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return this.originalResultSet.getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return this.originalResultSet.getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return this.originalResultSet.getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return this.originalResultSet.getArray(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return this.originalResultSet.getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return this.originalResultSet.getRef(columnLabel);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return this.originalResultSet.getBlob(columnLabel);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return this.originalResultSet.getClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return this.originalResultSet.getArray(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return this.originalResultSet.getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return this.originalResultSet.getDate(columnLabel, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return this.originalResultSet.getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return this.originalResultSet.getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return this.originalResultSet.getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return this.originalResultSet.getTimestamp(columnLabel, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return this.originalResultSet.getURL(columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return this.originalResultSet.getURL(columnLabel);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        this.originalResultSet.updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        this.originalResultSet.updateRef(columnLabel, x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        this.originalResultSet.updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        this.originalResultSet.updateBlob(columnLabel, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        this.originalResultSet.updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        this.originalResultSet.updateClob(columnLabel, x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        this.originalResultSet.updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        this.originalResultSet.updateArray(columnLabel, x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return this.originalResultSet.getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return this.originalResultSet.getRowId(columnLabel);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        this.originalResultSet.updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        this.originalResultSet.updateRowId(columnLabel, x);
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.originalResultSet.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.originalResultSet.isClosed();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        this.originalResultSet.updateNString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        this.originalResultSet.updateNString(columnLabel, nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        this.originalResultSet.updateNClob(columnIndex, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        this.originalResultSet.updateNClob(columnLabel, nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return this.originalResultSet.getNClob(columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return this.originalResultSet.getNClob(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return this.originalResultSet.getSQLXML(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return this.originalResultSet.getSQLXML(columnLabel);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        this.originalResultSet.updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        this.originalResultSet.updateSQLXML(columnLabel, xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return this.originalResultSet.getNString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return this.originalResultSet.getNString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return this.originalResultSet.getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return this.originalResultSet.getNCharacterStream(columnLabel);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        this.originalResultSet.updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        this.originalResultSet.updateNCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        this.originalResultSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        this.originalResultSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        this.originalResultSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        this.originalResultSet.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        this.originalResultSet.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        this.originalResultSet.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        this.originalResultSet.updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        this.originalResultSet.updateBlob(columnLabel, inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        this.originalResultSet.updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        this.originalResultSet.updateClob(columnLabel, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        this.originalResultSet.updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        this.originalResultSet.updateNClob(columnLabel, reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        this.originalResultSet.updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        this.originalResultSet.updateNCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        this.originalResultSet.updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        this.originalResultSet.updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        this.originalResultSet.updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        this.originalResultSet.updateAsciiStream(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        this.originalResultSet.updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        this.originalResultSet.updateCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        this.originalResultSet.updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        this.originalResultSet.updateBlob(columnLabel, inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        this.originalResultSet.updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        this.originalResultSet.updateClob(columnLabel, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        this.originalResultSet.updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        this.originalResultSet.updateNClob(columnLabel, reader);
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.originalResultSet.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.originalResultSet.isWrapperFor(iface);
    }
}
