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

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/*
 * TDengine only supports a subset of the standard SQL, thus this implementation of the
 * standard JDBC API contains more or less some adjustments customized for certain
 * compatibility needs.
 */
public class DatabaseMetaDataResultSet extends AbstractResultSet {

    private List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
    private List<TSDBResultSetRowData> rowDataList = new ArrayList<>();
    private TSDBResultSetRowData rowCursor;

    // position of cursor, starts from 0 as beforeFirst, increases as next() is called
    private int cursorRowNumber = 0;

    public void setRowDataList(List<TSDBResultSetRowData> rowDataList) {
        this.rowDataList = rowDataList;
    }

    public void setColumnMetaDataList(List<ColumnMetaData> columnMetaDataList) {
        this.columnMetaDataList = columnMetaDataList;
    }

    @Override
    public boolean next() throws SQLException {
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
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return rowCursor.getString(columnIndex, nativeType);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return rowCursor.getBoolean(columnIndex, nativeType);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return (byte) rowCursor.getInt(columnIndex, nativeType);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return (short) rowCursor.getInt(columnIndex, nativeType);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return rowCursor.getInt(columnIndex, nativeType);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return rowCursor.getLong(columnIndex, nativeType);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return rowCursor.getFloat(columnIndex, nativeType);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return rowCursor.getDouble(columnIndex, nativeType);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return (rowCursor.getString(columnIndex, nativeType)).getBytes();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        return rowCursor.getTimestamp(columnIndex, nativeType);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new TSDBResultSetMetaData(this.columnMetaDataList);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return rowCursor.getObject(columnIndex);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (ColumnMetaData colMetaData : this.columnMetaDataList) {
            if (colMetaData.getColName() != null && colMetaData.getColName().equals(columnLabel)) {
                return colMetaData.getColIndex();
            }
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        int colType = columnMetaDataList.get(columnIndex - 1).getColType();
        int nativeType = TSDBConstants.jdbcType2TaosType(colType);
        double value = rowCursor.getDouble(columnIndex, nativeType);
        return new BigDecimal(value);
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
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void afterLast() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean first() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean last() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
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
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean previous() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        //TODO: calendar is not used
        return getTimestamp(columnIndex);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

}
