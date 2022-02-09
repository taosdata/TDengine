package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractResultSet;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.entity.*;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class WSResultSet extends AbstractResultSet {
    private final Statement statement;
    private final Transport transport;
    // data
    private final List<WSJsonData> jsonResult = new ArrayList<>();
    private final List<WSBlockData> blockData = new ArrayList<>();
    // meta
    private final List<String> columnNames;
    private final List<Field> columns = new ArrayList<>();
    private final WSResultSetMetaData metaData;

    private final long id;
    private final FetchType fetchType;
    private final RequestFactory factory;

    private int numOfRows = 0;
    private int rowIndex = 0;
    private boolean isClosed;

    public WSResultSet(Statement statement, QueryResp response, String database, Transport transport,
                       FetchType fetchType, RequestFactory factory) throws SQLException {
        this.statement = statement;
        this.transport = transport;
        this.fetchType = fetchType;
        this.id = response.getId();
        this.factory = factory;
        columnNames = Arrays.asList(response.getFieldsNames());
        for (int i = 0; i < response.getFieldsCount(); i++) {
            String colName = columnNames.get(i);
            int taosType = response.getFieldsTypes()[i];
            int jdbcType = TSDBConstants.taosType2JdbcType(taosType);
            int length = response.getFieldsLengths()[i];
            columns.add(new Field(colName, jdbcType, length, "", taosType));
        }
        this.metaData = new WSResultSetMetaData(database, columns);

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

        Request request = factory.generateFetch(id);
        CompletableFuture<Response> send = transport.send(request);
        try {
            Response response = send.get();
            FetchResp fetchResp = (FetchResp) response;
            if (Code.SUCCESS.getCode() != fetchResp.getCode()) {
//                TODO reWrite error type
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0, fetchResp.getMessage());
            }
            this.reset();
            if (fetchResp.isCompleted()) {
                return false;
            }
            this.numOfRows = fetchResp.getRows();
            if (FetchType.JSON == fetchType) {
                Request jsonRequest = factory.generateFetchJson(id);
                CompletableFuture<Response> fetchFuture = transport.send(jsonRequest);
                Response resp = fetchFuture.get();
                FetchJsonResp jsonResp = (FetchJsonResp) resp;
//                for (Object[] object : jsonResp.getData()) {
//                    jsonResult.add(new WSJsonData(object));
//                }
            } else {
                // fetch block
                Request blockRequest = factory.generateFetchBlock(id);
                CompletableFuture<Response> fetchFuture = transport.send(blockRequest);
                Response resp = fetchFuture.get();
                FetchBlockResp blockResp = (FetchBlockResp) resp;
                //TODO
            }
            return true;
        } catch (InterruptedException | ExecutionException e) {
//            TODO reWrite error type
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
        }
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
//        checkAvailability(columnIndex, jsonResult.get(rowIndex));
        WSJsonData data = jsonResult.get(rowIndex);

        wasNull = data == null;
        return null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    public static class Field {
        String name;
        int type;
        int length;
        String note;
        int taos_type;

        public Field(String name, int type, int length, String note, int taos_type) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.note = note;
            this.taos_type = taos_type;
        }
    }
}
