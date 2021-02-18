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
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TSDBResultSet extends AbstractResultSet implements ResultSet {
    private TSDBJNIConnector jniConnector;

    private final TSDBStatement statement;
    private long resultSetPointer = 0L;
    private List<ColumnMetaData> columnMetaDataList = new ArrayList<>();

    private TSDBResultSetRowData rowData;
    private TSDBResultSetBlockData blockData;

    private boolean batchFetch = false;
    private boolean lastWasNull = false;
    private final int COLUMN_INDEX_START_VALUE = 1;

    public void setBatchFetch(boolean batchFetch) {
        this.batchFetch = batchFetch;
    }

    public Boolean getBatchFetch() {
        return this.batchFetch;
    }

    public void setColumnMetaDataList(List<ColumnMetaData> columnMetaDataList) {
        this.columnMetaDataList = columnMetaDataList;
    }

    public TSDBResultSetRowData getRowData() {
        return rowData;
    }

    public TSDBResultSet(TSDBStatement statement, TSDBJNIConnector connector, long resultSetPointer) throws SQLException {
        this.statement = statement;
        this.jniConnector = connector;
        this.resultSetPointer = resultSetPointer;

        int code = this.jniConnector.getSchemaMetaData(this.resultSetPointer, this.columnMetaDataList);
        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
        if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_RESULT_SET_NULL));
        }
        if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_NUM_OF_FIELDS_0));
        }
        this.rowData = new TSDBResultSetRowData(this.columnMetaDataList.size());
        this.blockData = new TSDBResultSetBlockData(this.columnMetaDataList, this.columnMetaDataList.size());
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
        long res = 0L;
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

    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return new BigDecimal(getLong(columnIndex));
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return getString(columnIndex).getBytes();
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

    public ResultSetMetaData getMetaData() throws SQLException {
        return new TSDBResultSetMetaData(this.columnMetaDataList);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        int colIndex = getTrueColumnIndex(columnIndex);

        if (!this.getBatchFetch()) {
            this.lastWasNull = this.rowData.wasNull(colIndex);
            return this.rowData.get(colIndex);
        } else {
            return this.blockData.get(colIndex);
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return this.getObject(this.findColumn(columnLabel));
    }

    public int findColumn(String columnLabel) throws SQLException {
        for (ColumnMetaData colMetaData : this.columnMetaDataList) {
            if (colMetaData.getColName() != null && colMetaData.getColName().equalsIgnoreCase(columnLabel)) {
                return colMetaData.getColIndex() + 1;
            }
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        int colIndex = getTrueColumnIndex(columnIndex);

        if (!this.getBatchFetch()) {
            this.lastWasNull = this.rowData.wasNull(colIndex);
            return new BigDecimal(this.rowData.getLong(colIndex, this.columnMetaDataList.get(colIndex).getColType()));
        } else {
            return new BigDecimal(this.blockData.getLong(colIndex));
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void afterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean first() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean last() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public int getRow() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean previous() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    public Statement getStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.statement;
    }

    public boolean isClosed() throws SQLException {
        //TODO: check if need release resources
        boolean isClosed = true;
        if (jniConnector != null) {
            isClosed = jniConnector.isResultsetClosed();
        }
        return isClosed;
    }

    public String getNString(int columnIndex) throws SQLException {
        int colIndex = getTrueColumnIndex(columnIndex);
        return (String) rowData.get(colIndex);
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
