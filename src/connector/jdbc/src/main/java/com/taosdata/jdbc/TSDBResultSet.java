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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TSDBResultSet extends AbstractResultSet implements ResultSet {
    private final TSDBJNIConnector jniConnector;
    private final TSDBStatement statement;
    private final long resultSetPointer;
    private List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
    private final TSDBResultSetRowData rowData;
    private final TSDBResultSetBlockData blockData;

    private boolean batchFetch;
    private boolean lastWasNull;
    private boolean isClosed;

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
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        }
        if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        }
        if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
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
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
            } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
            } else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
            } else return code != TSDBConstants.JNI_FETCH_END;
        } else {
            if (rowData != null) {
                this.rowData.clear();
            }
            int code = this.jniConnector.fetchRow(this.resultSetPointer, this.rowData);
            if (code == TSDBConstants.JNI_CONNECTION_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
            } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
            } else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
            } else if (code == TSDBConstants.JNI_FETCH_END) {
                return false;
            } else {
                return true;
            }
        }
    }

    public void close() throws SQLException {
        if (isClosed)
            return;
        if (this.jniConnector != null) {
            int code = this.jniConnector.freeResultSet(this.resultSetPointer);
            if (code == TSDBConstants.JNI_CONNECTION_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
            } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
            }
        }
        isClosed = true;
    }

    public boolean wasNull() throws SQLException {
        return this.lastWasNull;
    }

    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        String res = null;
        if (this.getBatchFetch())
            return this.blockData.getString(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = this.rowData.getString(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());
        }
        return res;
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        boolean res = false;
        if (this.getBatchFetch())
            return this.blockData.getBoolean(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = this.rowData.getBoolean(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());
        }
        return res;
    }

    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        byte res = 0;
        if (this.getBatchFetch())
            return (byte) this.blockData.getInt(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = (byte) this.rowData.getInt(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());
        }
        return res;
    }

    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        short res = 0;
        if (this.getBatchFetch())
            return (short) this.blockData.getInt(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = (short) this.rowData.getInt(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());
        }
        return res;
    }

    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        int res = 0;
        if (this.getBatchFetch())
            return this.blockData.getInt(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = this.rowData.getInt(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());
        }
        return res;
    }

    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        long res = 0L;
        if (this.getBatchFetch())
            return this.blockData.getLong(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = this.rowData.getLong(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());
        }
        return res;
    }

    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        float res = 0;
        if (this.getBatchFetch())
            return (float) this.blockData.getDouble(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull)
            res = this.rowData.getFloat(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());

        return res;
    }

    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        double res = 0;
        if (this.getBatchFetch())
            return this.blockData.getDouble(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = this.rowData.getDouble(columnIndex - 1, this.columnMetaDataList.get(columnIndex - 1).getColType());
        }
        return res;
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        Object value = this.rowData.get(columnIndex - 1);
        if (value == null)
            return null;

        int colType = this.columnMetaDataList.get(columnIndex - 1).getColType();
        switch (colType) {
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return Longs.toByteArray((Long) value);
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return Ints.toByteArray((int) value);
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return Shorts.toByteArray((Short) value);
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return new byte[]{(byte) value};
        }
        return value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Timestamp timestamp = getTimestamp(columnIndex);
        return timestamp == null ? null : new Date(timestamp.getTime());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Timestamp timestamp = getTimestamp(columnIndex);
        return timestamp == null ? null : new Time(timestamp.getTime());
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        Timestamp res = null;

        if (this.getBatchFetch())
            return this.blockData.getTimestamp(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            res = this.rowData.getTimestamp(columnIndex - 1);
        }
        return res;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return new TSDBResultSetMetaData(this.columnMetaDataList);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        Object res = null;
        if (this.getBatchFetch())
            return this.blockData.get(columnIndex - 1);

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        if (!lastWasNull) {
            int colType = this.columnMetaDataList.get(columnIndex - 1).getColType();
            if (colType == TSDBConstants.TSDB_DATA_TYPE_BINARY)
                res = ((String) this.rowData.get(columnIndex - 1)).getBytes();
            else
                res = this.rowData.get(columnIndex - 1);
        }
        return res;
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
        if (this.getBatchFetch())
            return new BigDecimal(this.blockData.getLong(columnIndex - 1));

        this.lastWasNull = this.rowData.wasNull(columnIndex - 1);
        BigDecimal res = null;
        if (!lastWasNull) {
            int colType = this.columnMetaDataList.get(columnIndex - 1).getColType();
            switch (colType) {
                case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                case TSDBConstants.TSDB_DATA_TYPE_INT:
                case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                    res = new BigDecimal(Long.valueOf(this.rowData.get(columnIndex - 1).toString()));
                    break;
                case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                    res = new BigDecimal(Double.valueOf(this.rowData.get(columnIndex - 1).toString()));
                    break;
                case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                    return new BigDecimal(((Timestamp) this.rowData.get(columnIndex - 1)).getTime());
                default:
                    res = new BigDecimal(this.rowData.get(columnIndex - 1).toString());
            }
        }
        return res;
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

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        //TODO：did not use the specified timezone in cal
        return getTimestamp(columnIndex);
    }

    public boolean isClosed() throws SQLException {
        if (isClosed)
            return true;
        if (jniConnector != null) {
            isClosed = jniConnector.isResultsetClosed();
        }
        return isClosed;
    }

    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

}
