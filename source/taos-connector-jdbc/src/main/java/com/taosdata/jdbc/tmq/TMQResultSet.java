package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.*;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TMQResultSet extends AbstractResultSet {
    private final TMQConnector jniConnector;
    private final long resultSetPointer;
    private final List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
    private final TSDBResultSetBlockData blockData;

    private String dbName;
    private String tableName;

    private boolean isClosed;

    public TMQResultSet(TMQConnector connector, long resultSetPointer, int timestampPrecision) {
        this.jniConnector = connector;
        this.resultSetPointer = resultSetPointer;
        this.timestampPrecision = timestampPrecision;
        this.blockData = new TSDBResultSetBlockData(this.columnMetaDataList, timestampPrecision);
    }

    public TMQResultSet(TMQConnector connector, long resultSetPointer, int timestampPrecision, String dbName, String tableName) {
        this.jniConnector = connector;
        this.resultSetPointer = resultSetPointer;
        this.timestampPrecision = timestampPrecision;
        this.dbName = dbName;
        this.tableName = tableName;
        this.blockData = new TSDBResultSetBlockData(this.columnMetaDataList, timestampPrecision);
    }

    public boolean next() throws SQLException {
        if (this.blockData.forward())
            return true;

        int code;
        this.columnMetaDataList.clear();
        code = this.jniConnector.fetchBlock(this.resultSetPointer, this.blockData, this.columnMetaDataList);
        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        } else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
        } else if (code == TSDBConstants.JNI_FETCH_END) {
            return false;
        } else {
            this.blockData.doSetByteArray();
            this.blockData.reset();
            return true;
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

        return this.blockData.wasNull;
    }

    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return this.blockData.getString(columnIndex - 1);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return this.blockData.getBoolean(columnIndex - 1);
    }

    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return (byte) this.blockData.getInt(columnIndex - 1);
    }

    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return (short) this.blockData.getInt(columnIndex - 1);
    }

    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return this.blockData.getInt(columnIndex - 1);
    }

    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return this.blockData.getLong(columnIndex - 1);
    }

    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return (float) this.blockData.getDouble(columnIndex - 1);
    }

    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return this.blockData.getDouble(columnIndex - 1);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return this.blockData.getBytes(columnIndex - 1);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return this.blockData.getTimestamp(columnIndex - 1);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return new TSDBResultSetMetaData(this.columnMetaDataList, dbName, tableName);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        Object o = this.blockData.get(columnIndex - 1);
        if (o instanceof Instant){
            return Timestamp.from((Instant) o);
        }
        return o;
    }

    public int findColumn(String columnLabel) throws SQLException {
        for (ColumnMetaData colMetaData : this.columnMetaDataList) {
            if (colMetaData.getColName() != null && colMetaData.getColName().equalsIgnoreCase(columnLabel)) {
                return colMetaData.getColIndex();
            }
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, this.columnMetaDataList.size());

        return BigDecimal.valueOf(this.blockData.getDouble(columnIndex - 1));
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

        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        //TODO：did not use the specified timezone in cal
        return getTimestamp(columnIndex);
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }
}
