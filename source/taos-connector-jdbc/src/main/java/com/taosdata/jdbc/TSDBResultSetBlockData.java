package com.taosdata.jdbc;

import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.BlockUtil;
import com.taosdata.jdbc.utils.DataTypeConvertUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.taosdata.jdbc.TSDBConstants.*;
import static com.taosdata.jdbc.utils.UnsignedDataUtils.*;

public class TSDBResultSetBlockData {
    private int numOfRows = 0;
    private int rowIndex = 0;

    private List<ColumnMetaData> columnMetaDataList;
    private ArrayList<List<Object>> colData;
    public boolean wasNull;

    private int timestampPrecision;
    private ByteBuffer buffer;
    final Semaphore semaphore = new Semaphore(0);
    public int returnCode = 0;

    public TSDBResultSetBlockData(List<ColumnMetaData> colMeta, int numOfCols, int timestampPrecision) {
        this.columnMetaDataList = colMeta;
        this.colData = new ArrayList<>(numOfCols);
        this.timestampPrecision = timestampPrecision;
    }

    public TSDBResultSetBlockData(List<ColumnMetaData> colMeta, int timestampPrecision) {
        this.columnMetaDataList = colMeta;
        this.colData = new ArrayList<>();
        this.timestampPrecision = timestampPrecision;
    }

    public TSDBResultSetBlockData() {
        this.colData = new ArrayList<>();
    }

    public void clear() {
        int size = this.colData.size();
        this.colData.clear();
        setNumOfCols(size);
    }

    public int getNumOfRows() {
        return this.numOfRows;
    }

    public void setNumOfRows(int numOfRows) {
        this.numOfRows = numOfRows;
    }

    public int getNumOfCols() {
        return this.colData.size();
    }

    public void setNumOfCols(int numOfCols) {
        this.colData = new ArrayList<>(numOfCols);
    }

    public boolean hasMore() {
        return this.rowIndex < this.numOfRows;
    }

    public boolean forward() {
        if (this.rowIndex > this.numOfRows) {
            return false;
        }

        return ((++this.rowIndex) < this.numOfRows);
    }

    public void reset() {
        this.rowIndex = 0;
    }
    public void setByteArray(byte[] value) {
        byte[] copy = new byte[value.length];
        System.arraycopy(value, 0, copy, 0, value.length);
        buffer = ByteBuffer.wrap(copy);
    }
    public void doSetByteArray() {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        int bitMapOffset = BitmapLen(numOfRows);
        int pHeader = 28 + columnMetaDataList.size() * 5;
        buffer.position(pHeader);
        List<Integer> lengths = new ArrayList<>(columnMetaDataList.size());
        for (int i = 0; i < columnMetaDataList.size(); i++) {
            lengths.add(buffer.getInt());
        }
        pHeader = buffer.position();
        int length = 0;
        for (int i = 0; i < columnMetaDataList.size(); i++) {
            List<Object> col = new ArrayList<>(numOfRows);
            int type = columnMetaDataList.get(i).getColType();
            switch (type) {
                case TSDB_DATA_TYPE_BOOL:
                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        byte b = buffer.get();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(b);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        short s = buffer.getShort();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(s);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        int in = buffer.getInt();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(in);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:{
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        long l = buffer.getLong();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(l);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        long l = buffer.getLong();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(DateTimeUtils.parseTimestampColumnData(l, this.timestampPrecision));
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        float f = buffer.getFloat();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(f);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.get(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        double d = buffer.getDouble();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(d);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_JSON:
                case TSDB_DATA_TYPE_BLOB:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_GEOMETRY:{
                    length = numOfRows * 4;
                    List<Integer> offset = new ArrayList<>(numOfRows);
                    for (int m = 0; m < numOfRows; m++) {
                        offset.add(buffer.getInt());
                    }
                    int start = buffer.position();
                    for (int m = 0; m < numOfRows; m++) {
                        if (-1 == offset.get(m)) {
                            col.add(null);
                            continue;
                        }
                        buffer.position(start + offset.get(m));
                        int len;
                        if (type != TSDB_DATA_TYPE_BLOB) {
                            len = buffer.getShort() & 0xFFFF;

                        } else {
                            len = buffer.getInt();
                        }
                        byte[] tmp = new byte[len];
                        buffer.get(tmp);
                        col.add(tmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_NCHAR: {
                    length = numOfRows * 4;
                    List<Integer> offset = new ArrayList<>(numOfRows);
                    for (int m = 0; m < numOfRows; m++) {
                        offset.add(buffer.getInt());
                    }
                    int start = buffer.position();
                    for (int m = 0; m < numOfRows; m++) {
                        if (-1 == offset.get(m)) {
                            col.add(null);
                            continue;
                        }
                        buffer.position(start + offset.get(m));
                        int len = (buffer.getShort() & 0xFFFF) / 4;
                        int[] tmp = new int[len];
                        for (int n = 0; n < len; n++) {
                            tmp[n] = buffer.getInt();
                        }
                        col.add(new String(tmp, 0, tmp.length));
                    }
                    break;
                }
                default:
                    // unknown type, do nothing
                    col.add(null);
                    break;
            }
            pHeader += length + lengths.get(i);
            buffer.position(pHeader);
            colData.add(col);
        }
        semaphore.release();
    }

    public void doneWithNoData(){
        semaphore.release();
    }

    public void waitTillOK() throws SQLException {
        try {
            // must be ok When the CPU has idle time
            if (!semaphore.tryAcquire(5, TimeUnit.SECONDS))
            {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "FETCH DATA TIME OUT");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * The original type may not be a string type, but will be converted to by
     * calling this method
     */
    public String getString(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return DataTypeConvertUtil.getString(obj);
    }

    public byte[] getBytes(int col) throws SQLException {

        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return DataTypeConvertUtil.getBytes(obj);
    }

    public int getInt(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        return DataTypeConvertUtil.getInt(type, obj, col);
    }

    public boolean getBoolean(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return Boolean.FALSE;
        }

        if (obj instanceof  Boolean) {
            return (Boolean) obj;
        }

        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        return DataTypeConvertUtil.getBoolean(type, obj);
    }

    public long getLong(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (obj instanceof Long) {
            return (long) obj;
        }
        int type = this.columnMetaDataList.get(col).getColType();
        return DataTypeConvertUtil.getLong(type, obj, col, this.timestampPrecision);
    }

    private void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
                "'" + valueAsString + "' in column '" + columnIndex + "' is outside valid range for the jdbcType " + jdbcType2TaosTypeName(jdbcType));
    }

    public Timestamp getTimestamp(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        if (type == TSDB_DATA_TYPE_BIGINT) {
            Instant instant = DateTimeUtils.parseTimestampColumnData((long) obj, this.timestampPrecision);
            return DateTimeUtils.getTimestamp(instant, null);
        }
        if (type == TSDB_DATA_TYPE_TIMESTAMP)
            return DateTimeUtils.getTimestamp((Instant) obj, null);
        if (obj instanceof byte[]) {
            String tmp = "";
            String charset = TaosGlobalConfig.getCharset();
            try {
                tmp = new String((byte[]) obj, charset);
                return DateTimeUtils.parseTimestamp(tmp, null);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return new Timestamp(getLong(col));
    }

    public double getDouble(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        int type = this.columnMetaDataList.get(col).getColType();
        return DataTypeConvertUtil.getDouble(type, obj, col, this.timestampPrecision);
    }

    public Object get(int col) {
        List<Object> bb = this.colData.get(col);

        Object source = bb.get(this.rowIndex);
        if (null == source) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        switch (this.columnMetaDataList.get(col).getColType()) {
            case TSDB_DATA_TYPE_BOOL: {
                byte val = (byte) source;
                return (val == 0x0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_FLOAT:
            case TSDB_DATA_TYPE_DOUBLE:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_TIMESTAMP:
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:{
                return source;
            }
            case TSDB_DATA_TYPE_BLOB:{
                return new TDBlob((byte[]) source, true);
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                byte val = (byte) source;
                return parseUTinyInt(val);
            }
            case TSDB_DATA_TYPE_USMALLINT: {
                short val = (short) source;
                return parseUSmallInt(val);
            }
            case TSDB_DATA_TYPE_UINT: {
                int val = (int) source;
                return parseUInteger(val);
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                long val = (long) source;
                return parseUBigInt(val);
            }
            default:
                // unknown type, do nothing
                return null;
        }
    }

    // ceil(numOfRows/8.0)
    private int BitmapLen(int n) {
        return (n + 0x7) >> 3;
    }

}
