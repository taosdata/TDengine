package com.taosdata.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.*;

import static com.taosdata.jdbc.TSDBConstants.JNI_SUCCESS;

public class TSDBResultSet extends AbstractResultSet {
    private final TSDBJNIConnector jniConnector;
    private final TSDBStatement statement;
    private final long resultSetPointer;
    private List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
    private final int cacheSize = 5;
    final BlockingQueue<TSDBResultSetBlockData> blockingQueueOut = new LinkedBlockingQueue<>(cacheSize);
    private TSDBResultSetBlockData blockData;
    private volatile boolean isClosed;
    private int fetchBlockNum = 0;
    ThreadPoolExecutor backFetchExecutor;
    final ForkJoinPool dataHandleExecutor = getForkJoinPool();


    public void setColumnMetaDataList(List<ColumnMetaData> columnMetaDataList) {
        this.columnMetaDataList = columnMetaDataList;
    }

    public TSDBResultSet(TSDBStatement statement, TSDBJNIConnector connector, long resultSetPointer, int timestampPrecision) throws SQLException {
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

        this.timestampPrecision = timestampPrecision;
        this.blockData = new TSDBResultSetBlockData();

    }

    private void startBackendFetch() {
        backFetchExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(1);
        backFetchExecutor.submit(() -> {
            try {
                while (!isClosed){
                    TSDBResultSetBlockData tsdbResultSetBlockData = new TSDBResultSetBlockData(this.columnMetaDataList, this.columnMetaDataList.size(), timestampPrecision);
                    tsdbResultSetBlockData.returnCode = this.jniConnector.fetchBlock(this.resultSetPointer, tsdbResultSetBlockData);

                    while (!blockingQueueOut.offer(tsdbResultSetBlockData, 10, TimeUnit.MILLISECONDS)) {
                        if (isClosed) {
                            return;
                        }
                    }
                    if (tsdbResultSetBlockData.returnCode == JNI_SUCCESS) {
                        dataHandleExecutor.submit(tsdbResultSetBlockData::doSetByteArray);
                    } else {
                        tsdbResultSetBlockData.doneWithNoData();
                        break;
                    }
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
    }
    public boolean next() throws SQLException {
        if (isClosed){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        }

        if (this.blockData.forward())
            return true;

        fetchBlockNum++;

        int code;
        if (fetchBlockNum > START_BACKEND_FETCH_BLOCK_NUM) {
            if (backFetchExecutor == null) {
                startBackendFetch();
            }

            try {
                this.blockData = blockingQueueOut.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Thread was interrupted while waiting for data", e);
            }
            this.blockData.waitTillOK();

            code = this.blockData.returnCode;
        } else {
            TSDBResultSetBlockData tsdbResultSetBlockData = new TSDBResultSetBlockData(this.columnMetaDataList, this.columnMetaDataList.size(), timestampPrecision);

            code = this.jniConnector.fetchBlock(this.resultSetPointer, tsdbResultSetBlockData);
            this.blockData = tsdbResultSetBlockData;
            if (code == JNI_SUCCESS) {
                this.blockData.doSetByteArray();
            }
            this.blockData.reset();
        }

        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        } else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
        } else return code != TSDBConstants.JNI_FETCH_END;
    }

    public void close() throws SQLException {
        if (isClosed)
            return;
        isClosed = true;

        if (backFetchExecutor != null) {
            while (backFetchExecutor.getActiveCount() != 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            if (!backFetchExecutor.isShutdown()) {
                backFetchExecutor.shutdown();
            }
        }

        if (this.statement == null)
            return;
        if (this.jniConnector != null) {
            int code = this.jniConnector.freeResultSet(this.resultSetPointer);
            if (code == TSDBConstants.JNI_CONNECTION_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
            } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
            }
        }
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

        return new TSDBResultSetMetaData(this.columnMetaDataList);
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
        return BigDecimal.valueOf(this.blockData.getDouble(columnIndex - 1));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
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
        return isClosed;
    }

    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }
}
