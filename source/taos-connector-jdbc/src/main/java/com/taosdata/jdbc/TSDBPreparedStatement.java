package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/*
 * TDengine only supports a subset of the standard SQL, thus this implementation of the
 * standard JDBC API contains more or less some adjustments customized for certain
 * compatibility needs.
 */
public class TSDBPreparedStatement extends TSDBStatement implements TaosPrepareStatement {
    // for jdbc preparedStatement interface
    private final String rawSql;
    private Object[] parameters = new Object[0];
    // for parameter binding
    private long nativeStmtHandle;
    private String tableName;
    private List<TableTagInfo> tableTags;
    private int tagValueLength;
    private final PriorityQueue<ColumnInfo> queue = new PriorityQueue<>();



    TSDBPreparedStatement(TSDBConnection connection, String sql, Long instanceId) throws SQLException {
        super(connection, instanceId);
        this.rawSql = sql;
        int parameterCnt = 0;
        if (!sql.contains("?"))
            return;
        for (int i = 0; i < sql.length(); i++) {
            if ('?' == sql.charAt(i)) {
                parameterCnt++;
            }
        }
        parameters = new Object[parameterCnt];
        // for parameter-binding
        TSDBJNIConnector connector = ((TSDBConnection) this.getConnection()).getConnector();
        this.nativeStmtHandle = connector.prepareStmt(rawSql);

        // the table name is also a parameter, so ignore it.
        this.tableTags = new ArrayList<>();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        final String sql = Utils.getNativeSql(this.rawSql, this.parameters);
        return executeQuery(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        String sql = Utils.getNativeSql(this.rawSql, this.parameters);
        return executeUpdate(sql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x.doubleValue());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setObject(parameterIndex, new Timestamp(x.getTime()));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setObject(parameterIndex, new Timestamp(x.getTime()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void clearParameters() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        parameters = new Object[parameters.length];
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (parameterIndex < 1 || parameterIndex > parameters.length)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public boolean execute() throws SQLException {
        final String sql = Utils.getNativeSql(this.rawSql, this.parameters);
        return execute(sql);
    }

    @Override
    public void addBatch() throws SQLException {
        String sql = Utils.getNativeSql(this.rawSql, this.parameters);
        addBatch(sql);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (this.getResultSet() == null)
            return null;
        return getResultSet().getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return new TSDBParameterMetaData(parameters);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    ///////////////////////////////////////////////////////////////////////
    // NOTE: the following APIs are not JDBC compatible
    // parameter binding
    private static class ColumnInfo implements Comparable<ColumnInfo> {
        @SuppressWarnings("rawtypes")
        private List data;
        private int type;
        private int bytes;
        private boolean typeIsSet;
        private int index;

        public ColumnInfo() {
            this.typeIsSet = false;
        }

        public void setType(int type) throws SQLException {
            if (this.isTypeSet()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "column data type has been set");
            }

            this.typeIsSet = true;
            this.type = type;
        }

        public boolean isTypeSet() {
            return this.typeIsSet;
        }

        @Override
        public int compareTo(ColumnInfo c) {
            return this.index > c.index ? 1 : -1;
        }
    }

    private static class TableTagInfo {
        private boolean isNull;
        private final Object value;
        private final int type;

        public TableTagInfo(Object value, int type) {
            this.value = value;
            this.type = type;
        }

        public static TableTagInfo createNullTag(int type) {
            TableTagInfo info = new TableTagInfo(null, type);
            info.isNull = true;
            return info;
        }
    }

    @Override
    public void setTableName(String name) throws SQLException {

        if (this.nativeStmtHandle == 0) {
            TSDBJNIConnector connector = ((TSDBConnection) this.getConnection()).getConnector();
            this.nativeStmtHandle = connector.prepareStmt(rawSql);
        }

        if (this.tableName != null) {
            this.columnDataAddBatch();
            this.columnDataClearBatchInternal();
        }
        this.tableName = name;
    }

    private void ensureTagCapacity(int index) {
        if (this.tableTags.size() < index + 1) {
            int delta = index + 1 - this.tableTags.size();
            this.tableTags.addAll(Collections.nCopies(delta, null));
        }
    }

    @Override
    public void setTagNull(int index, int type) {
        ensureTagCapacity(index);
        this.tableTags.set(index, TableTagInfo.createNullTag(type));
    }

    @Override
    public void setTagBoolean(int index, boolean value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_BOOL));
        this.tagValueLength += Byte.BYTES;
    }

    @Override
    public void setTagInt(int index, int value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_INT));
        this.tagValueLength += Integer.BYTES;
    }

    @Override
    public void setTagByte(int index, byte value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_TINYINT));
        this.tagValueLength += Byte.BYTES;
    }

    @Override
    public void setTagShort(int index, short value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_SMALLINT));
        this.tagValueLength += Short.BYTES;
    }

    @Override
    public void setTagLong(int index, long value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_BIGINT));
        this.tagValueLength += Long.BYTES;
    }

    @Override
    public void setTagBigInteger(int index, BigInteger value) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTagTimestamp(int index, long value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP));
        this.tagValueLength += Long.BYTES;
    }

    @Override
    public void setTagTimestamp(int index, Timestamp value) {
        throw new RuntimeException("not supported");
    }

    @Override
    public void setTagFloat(int index, float value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_FLOAT));
        this.tagValueLength += Float.BYTES;
    }

    @Override
    public void setTagDouble(int index, double value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_DOUBLE));
        this.tagValueLength += Double.BYTES;
    }

    @Override
    public void setTagString(int index, String value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_BINARY));
        this.tagValueLength += value.getBytes().length;
    }

    @Override
    public void setTagNString(int index, String value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_NCHAR));

        String charset = TaosGlobalConfig.getCharset();
        try {
            this.tagValueLength += value.getBytes(charset).length;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void setTagJson(int index, String value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_JSON));

        String charset = TaosGlobalConfig.getCharset();
        try {
            this.tagValueLength += value.getBytes(charset).length;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void setTagVarbinary(int index, byte[] value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_VARBINARY));
        this.tagValueLength += value.length;
     }
    @Override
    public void setTagGeometry(int index, byte[] value) {
        ensureTagCapacity(index);
        this.tableTags.set(index, new TableTagInfo(value, TSDBConstants.TSDB_DATA_TYPE_GEOMETRY));
        this.tagValueLength += value.length;
    }

    public <T> void setValueImpl(int columnIndex, List<T> list, int type, int bytes) throws SQLException {
        ColumnInfo p = new ColumnInfo();
        p.setType(type);
        p.bytes = bytes;
        p.data = list;
        p.index = columnIndex;
        queue.add(p);
    }

    @Override
    public void setInt(int columnIndex, List<Integer> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_INT, Integer.BYTES);
    }
    @Override
    public void setFloat(int columnIndex, List<Float> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_FLOAT, Float.BYTES);
    }

    @Override
    public void setTimestamp(int columnIndex, List<Long> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, Long.BYTES);
    }

    @Override
    public void setLong(int columnIndex, List<Long> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BIGINT, Long.BYTES);
    }

    @Override
    public void setBigInteger(int columnIndex, List<BigInteger> list) throws SQLException{
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBigDecimal(int columnIndex, List<BigDecimal> list) throws SQLException{
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    @Override
    public void setDouble(int columnIndex, List<Double> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_DOUBLE, Double.BYTES);
    }

    @Override
    public void setBoolean(int columnIndex, List<Boolean> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BOOL, Byte.BYTES);
    }

    public void setByte(int columnIndex, List<Byte> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_TINYINT, Byte.BYTES);
    }

    @Override
    public void setShort(int columnIndex, List<Short> list) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_SMALLINT, Short.BYTES);
    }

    @Override
    public void setString(int columnIndex, List<String> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BINARY, size);
    }

    @Override
    public void setVarbinary(int columnIndex, List<byte[]> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_VARBINARY, size);
    }

    @Override
    public void setGeometry(int columnIndex, List<byte[]> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_GEOMETRY, size);
    }

    @Override
    public void setBlob(int columnIndex, List<Blob> list, int size) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "Blob type is not supported in native parameter binding");
    }

    // note: expand the required space for each NChar character
    @Override
    public void setNString(int columnIndex, List<String> list, int size) throws SQLException {
        setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_NCHAR, size * Integer.BYTES);
    }

    @Override
    public void columnDataAddBatch() throws SQLException {
        // pass the data block to native code
        if (rawSql == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "sql statement not set yet");
        }

        int numOfCols = this.queue.size();
        if (numOfCols == 0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "column data not bind");
        }
        if (nativeStmtHandle == 0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "stmt is null");
        }

        TSDBJNIConnector connector = ((TSDBConnection) this.getConnection()).getConnector();
        if ((this.tableTags == null || this.tableTags.size() == 0) && this.tableName != null) {
            connector.setBindTableName(this.nativeStmtHandle, this.tableName);
        } else if (this.tableTags != null && this.tableTags.size() > 0) {
            if (tableName == null){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "table name not set yet");
            }

            int tagSize = this.tableTags.size();
            ByteBuffer tagDataList = ByteBuffer.allocate(this.tagValueLength);
            tagDataList.order(ByteOrder.LITTLE_ENDIAN);

            ByteBuffer typeList = ByteBuffer.allocate(tagSize);
            typeList.order(ByteOrder.LITTLE_ENDIAN);

            ByteBuffer lengthList = ByteBuffer.allocate(tagSize * Integer.BYTES);
            lengthList.order(ByteOrder.LITTLE_ENDIAN);

            ByteBuffer isNullList = ByteBuffer.allocate(tagSize * Byte.BYTES);
            isNullList.order(ByteOrder.LITTLE_ENDIAN);

            for (TableTagInfo tag : this.tableTags) {
                if (tag.isNull) {
                    typeList.put((byte) tag.type);
                    isNullList.put((byte) 1);
                    lengthList.putInt(0);
                    continue;
                }

                switch (tag.type) {
                    case TSDBConstants.TSDB_DATA_TYPE_INT: {
                        Integer val = (Integer) tag.value;
                        tagDataList.putInt(val);
                        lengthList.putInt(Integer.BYTES);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_TINYINT: {
                        Byte val = (Byte) tag.value;
                        tagDataList.put(val);
                        lengthList.putInt(Byte.BYTES);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_BOOL: {
                        Boolean val = (Boolean) tag.value;
                        tagDataList.put((byte) (val ? 1 : 0));
                        lengthList.putInt(Byte.BYTES);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_SMALLINT: {
                        Short val = (Short) tag.value;
                        tagDataList.putShort(val);
                        lengthList.putInt(Short.BYTES);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                    case TSDBConstants.TSDB_DATA_TYPE_BIGINT: {
                        Long val = (Long) tag.value;
                        tagDataList.putLong(val == null ? 0 : val);
                        lengthList.putInt(Long.BYTES);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
                        Float val = (Float) tag.value;
                        tagDataList.putFloat(val == null ? 0 : val);
                        lengthList.putInt(Float.BYTES);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                        Double val = (Double) tag.value;
                        tagDataList.putDouble(val == null ? 0 : val);
                        lengthList.putInt(Double.BYTES);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                    case TSDBConstants.TSDB_DATA_TYPE_JSON:
                    case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
                        String charset = TaosGlobalConfig.getCharset();
                        String val = (String) tag.value;
                        byte[] b;
                        try {
                            if (tag.type == TSDBConstants.TSDB_DATA_TYPE_BINARY) {
                                b = val.getBytes();
                            } else {
                                b = val.getBytes(charset);
                            }
                        } catch (UnsupportedEncodingException e) {
                            throw new RuntimeException(e.getMessage());
                        }
                        tagDataList.put(b);
                        lengthList.putInt(b.length);
                        break;
                    }

                    case TSDBConstants.TSDB_DATA_TYPE_VARBINARY:
                    case TSDBConstants.TSDB_DATA_TYPE_GEOMETRY: {
                        byte[] val = (byte[]) tag.value;
                        tagDataList.put(val);
                        lengthList.putInt(val.length);
                        break;
                    }
                    case TSDBConstants.TSDB_DATA_TYPE_UTINYINT:
                    case TSDBConstants.TSDB_DATA_TYPE_USMALLINT:
                    case TSDBConstants.TSDB_DATA_TYPE_UINT:
                    case TSDBConstants.TSDB_DATA_TYPE_UBIGINT: {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "not support data types");
                    }
                }
                typeList.put((byte) tag.type);
                isNullList.put(tag.isNull ? (byte) 1 : (byte) 0);
            }

            connector.setBindTableNameAndTags(this.nativeStmtHandle, this.tableName, this.tableTags.size(),
                    tagDataList, typeList, lengthList, isNullList);
        }

        ArrayList<ColumnInfo> colData = new ArrayList<>();

        for (int i = 0; i < numOfCols; i++) {
            colData.add(queue.poll());
        }
        ColumnInfo colInfo = colData.get(0);
        if (colInfo == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "column data not bind");
        }

        int rows = colInfo.data.size();
        for (int i = 0; i < numOfCols; ++i) {
            ColumnInfo col1 = colData.get(i);
            if (col1 == null || !col1.isTypeSet()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "column data not bind");
            }
            if (rows != col1.data.size()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "the rows in column data not identical");
            }

            ByteBuffer colDataList = ByteBuffer.allocate(rows * col1.bytes);
            colDataList.order(ByteOrder.LITTLE_ENDIAN);

            ByteBuffer lengthList = ByteBuffer.allocate(rows * Integer.BYTES);
            lengthList.order(ByteOrder.LITTLE_ENDIAN);

            ByteBuffer isNullList = ByteBuffer.allocate(rows * Byte.BYTES);
            isNullList.order(ByteOrder.LITTLE_ENDIAN);

            switch (col1.type) {
                case TSDBConstants.TSDB_DATA_TYPE_INT: {
                    for (int j = 0; j < rows; ++j) {
                        Integer val = (Integer) col1.data.get(j);
                        colDataList.putInt(val == null ? Integer.MIN_VALUE : val);
                        isNullList.put((byte) (val == null ? 1 : 0));
                    }
                    break;
                }

                case TSDBConstants.TSDB_DATA_TYPE_TINYINT: {
                    for (int j = 0; j < rows; ++j) {
                        Byte val = (Byte) col1.data.get(j);
                        colDataList.put(val == null ? 0 : val);
                        isNullList.put((byte) (val == null ? 1 : 0));
                    }
                    break;
                }

                case TSDBConstants.TSDB_DATA_TYPE_BOOL: {
                    for (int j = 0; j < rows; ++j) {
                        Boolean val = (Boolean) col1.data.get(j);
                        if (val == null) {
                            colDataList.put((byte) 0);
                        } else {
                            colDataList.put((byte) (val ? 1 : 0));
                        }

                        isNullList.put((byte) (val == null ? 1 : 0));
                    }
                    break;
                }

                case TSDBConstants.TSDB_DATA_TYPE_SMALLINT: {
                    for (int j = 0; j < rows; ++j) {
                        Short val = (Short) col1.data.get(j);
                        colDataList.putShort(val == null ? 0 : val);
                        isNullList.put((byte) (val == null ? 1 : 0));
                    }
                    break;
                }

                case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                case TSDBConstants.TSDB_DATA_TYPE_BIGINT: {
                    for (int j = 0; j < rows; ++j) {
                        Long val = (Long) col1.data.get(j);
                        colDataList.putLong(val == null ? 0 : val);
                        isNullList.put((byte) (val == null ? 1 : 0));
                    }
                    break;
                }

                case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
                    for (int j = 0; j < rows; ++j) {
                        Float val = (Float) col1.data.get(j);
                        colDataList.putFloat(val == null ? 0 : val);
                        isNullList.put((byte) (val == null ? 1 : 0));
                    }
                    break;
                }

                case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                    for (int j = 0; j < rows; ++j) {
                        Double val = (Double) col1.data.get(j);
                        colDataList.putDouble(val == null ? 0 : val);
                        isNullList.put((byte) (val == null ? 1 : 0));
                    }
                    break;
                }

                case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                case TSDBConstants.TSDB_DATA_TYPE_BINARY:{
                    String charset = TaosGlobalConfig.getCharset();
                    for (int j = 0; j < rows; ++j) {
                        String val = (String) col1.data.get(j);
                        colDataList.position(j * col1.bytes);  // seek to the correct position
                        if (val != null) {
                            byte[] b;
                            try {
                                if (col1.type == TSDBConstants.TSDB_DATA_TYPE_BINARY) {
                                    b = val.getBytes();
                                } else {
                                    b = val.getBytes(charset);
                                }
                            } catch (UnsupportedEncodingException e) {
                                throw new RuntimeException(e.getMessage());
                            }

                            if (val.length() > col1.bytes) {
                                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "string data too long");
                            }

                            colDataList.put(b);
                            lengthList.putInt(b.length);
                            isNullList.put((byte) 0);
                        } else {
                            lengthList.putInt(0);
                            isNullList.put((byte) 1);
                        }
                    }
                    break;
                }
                case TSDBConstants.TSDB_DATA_TYPE_VARBINARY:
                case TSDBConstants.TSDB_DATA_TYPE_GEOMETRY: {
                    for (int j = 0; j < rows; ++j) {
                        byte[] val = (byte[]) col1.data.get(j);
                        colDataList.position(j * col1.bytes);  // seek to the correct position
                        if (val != null) {
                            if (val.length > col1.bytes) {
                                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "varbinary/geometry data too long");
                            }
                            colDataList.put(val);
                            lengthList.putInt(val.length);
                            isNullList.put((byte) 0);
                        } else {
                            lengthList.putInt(0);
                            isNullList.put((byte) 1);
                        }
                    }
                    break;
                }
                case TSDBConstants.TSDB_DATA_TYPE_UTINYINT:
                case TSDBConstants.TSDB_DATA_TYPE_USMALLINT:
                case TSDBConstants.TSDB_DATA_TYPE_UINT:
                case TSDBConstants.TSDB_DATA_TYPE_UBIGINT: {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "not support data types");
                }
            }

            connector.bindColumnDataArray(this.nativeStmtHandle, colDataList, lengthList, isNullList, col1.type, col1.bytes, rows, i);
        }
        connector.addBatch(this.nativeStmtHandle);
        this.columnDataClearBatchInternal();
    }

    public void columnDataExecuteBatch() throws SQLException {
        TSDBJNIConnector connector = ((TSDBConnection) this.getConnection()).getConnector();
        connector.executeBatch(this.nativeStmtHandle);
        this.columnDataClearBatchInternal();
    }

    @Deprecated
    public void columnDataClearBatch() {
        columnDataClearBatchInternal();
    }

    private void columnDataClearBatchInternal() {
        this.tableName = null;
        if (this.tableTags != null)
            this.tableTags.clear();
        tagValueLength = 0;
    }

    public void columnDataCloseBatch() throws SQLException {
        TSDBJNIConnector connector = ((TSDBConnection) this.getConnection()).getConnector();
        connector.closeBatch(this.nativeStmtHandle);

        this.nativeStmtHandle = 0L;
        this.tableName = null;
    }

    @Override
    public void close() throws SQLException {
        if (this.nativeStmtHandle != 0L) {
            this.columnDataClearBatchInternal();
            this.columnDataCloseBatch();
        }
        super.close();
    }
}
