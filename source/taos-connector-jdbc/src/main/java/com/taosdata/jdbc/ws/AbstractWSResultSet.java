package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.BlockData;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.utils.FetchDataUtil;
import com.taosdata.jdbc.ws.entity.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractWSResultSet extends AbstractResultSet {

    protected final Statement statement;
    protected final Transport transport;
    protected final long queryId;
    protected final long reqId;

    protected volatile boolean isClosed;
    private boolean isCompleted = false;
    // meta
    protected final ResultSetMetaData metaData;
    protected final List<RestfulResultSet.Field> fields = new ArrayList<>();
    protected final List<String> columnNames;
    // data
    protected List<List<Object>> result = new ArrayList<>();

    protected int numOfRows = 0;
    protected int rowIndex = 0;
    private final FetchBlockData fetchBlockData;
    protected AbstractWSResultSet(Statement statement, Transport transport,
                               QueryResp response, String database) throws SQLException {
        this.statement = statement;
        this.transport = transport;
        this.queryId = response.getId();
        this.reqId = response.getReqId();
        columnNames = Arrays.asList(response.getFieldsNames());
        for (int i = 0; i < response.getFieldsCount(); i++) {
            String colName = response.getFieldsNames()[i];
            int taosType = response.getFieldsTypes()[i];
            int jdbcType = DataType.convertTaosType2DataType(taosType).getJdbcTypeValue();
            int length = response.getFieldsLengths()[i];
            int scale = 0;
            int precision = 0;

            if (response.getFieldsScales() != null) {
                scale = response.getFieldsScales()[i];
            }
            if (response.getFieldsPrecisions() != null) {
                precision = response.getFieldsPrecisions()[i];
            }
            fields.add(new RestfulResultSet.Field(colName, jdbcType, length, "", taosType, scale, precision));
        }
        this.metaData = new RestfulResultSetMetaData(database, fields, transport.getConnectionParam().isVarcharAsString());
        this.timestampPrecision = response.getPrecision();
        fetchBlockData = new FetchBlockData(transport, response, fields, transport.getConnectionParam().getRequestTimeout(), timestampPrecision);
        FetchDataUtil.getFetchMap().put(reqId, fetchBlockData);
    }

    private boolean forward() {
        if (this.rowIndex > this.numOfRows) {
            return false;
        }

        return ((++this.rowIndex) < this.numOfRows);
    }

    public void reset() {
        this.rowIndex = 0;
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        }

        if (this.forward()) {
            return true;
        }

        if (this.isCompleted) {
            return false;
        }

        BlockData blockData = fetchBlockData.getBlockData();

        if (blockData.getReturnCode() != Code.SUCCESS.getCode()) {
            throw TSDBError.createSQLException(blockData.getReturnCode(), "FETCH DATA ERROR:" + blockData.getErrorMessage());
        }
        this.reset();
        if (blockData.isCompleted()) {
            this.isCompleted = true;
            return false;
        }
        blockData.waitTillOK();
        this.result = blockData.getData();
        this.numOfRows = blockData.getNumOfRows();

        return true;
    }

    @Override
    public void close() throws SQLException {
        synchronized (this) {
            if (!this.isClosed) {
                this.isClosed = true;
                FetchDataUtil.getFetchMap().remove(reqId);

                if (!isCompleted) {
                    FreeResultReq closeReq = new FreeResultReq();
                    closeReq.setReqId(queryId);
                    closeReq.setId(queryId);
                    transport.sendWithoutResponse(new Request(Action.FREE_RESULT.getAction(), closeReq));
                }
            }
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.metaData;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }
}
