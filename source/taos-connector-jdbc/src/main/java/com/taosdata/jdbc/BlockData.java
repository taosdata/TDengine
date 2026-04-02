package com.taosdata.jdbc;

import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.utils.BlockUtil;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.DecimalUtil;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.taosdata.jdbc.TSDBConstants.*;

public class BlockData {
    private static final Logger log = LoggerFactory.getLogger(BlockData.class);
    private List<List<Object>> data;

    private int returnCode;
    private String errorMessage;
    private boolean isCompleted;
    private int numOfRows;
    private ByteBuf buffer;
    private List<RestfulResultSet.Field> fields;
    private final Semaphore semaphore;
    private final int precision;

    public BlockData(List<List<Object>> data,
                     int returnCode,
                     String errorMessage,
                     int numOfRows,
                     ByteBuf buffer,
                     List<RestfulResultSet.Field> fields,
                     int precision) {
        this.data = data;
        this.returnCode = returnCode;
        this.errorMessage = errorMessage;
        this.numOfRows = numOfRows;
        this.buffer = buffer;
        this.fields = fields;
        this.semaphore = new Semaphore(0);
        this.isCompleted = false;
        this.precision = precision;
    }

    public static BlockData getEmptyBlockData(List<RestfulResultSet.Field> fields, int precision) {
        return new BlockData(null, 0, "",0, null, fields, precision);
    }

    public void handleData() {
        try {
            int columns = fields.size();
            List<List<Object>> list = new ArrayList<>();
            if (buffer != null) {
                buffer.readIntLE(); // buffer length
                int pHeader = buffer.readerIndex() + 28 + columns * 5;
                buffer.skipBytes(8);
                this.numOfRows = buffer.readIntLE();

                buffer.readerIndex(pHeader);
                int bitMapOffset = bitmapLen(numOfRows);

                List<Integer> lengths = new ArrayList<>(columns);
                for (int i = 0; i < columns; i++) {
                    lengths.add(buffer.readIntLE());
                }
                pHeader = buffer.readerIndex();
                int length = 0;
                for (int i = 0; i < columns; i++) {
                    List<Object> col = new ArrayList<>(numOfRows);
                    int type = fields.get(i).getTaosType();
                    int scale = fields.get(i).getScale();
                    switch (type) {
                        case TSDB_DATA_TYPE_BOOL:
                        case TSDB_DATA_TYPE_TINYINT:
                        case TSDB_DATA_TYPE_UTINYINT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                byte b = buffer.readByte();
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
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                short s = buffer.readShortLE();
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
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                int in = buffer.readIntLE();
                                if (BlockUtil.isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    col.add(in);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_BIGINT:
                        case TSDB_DATA_TYPE_UBIGINT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                long l = buffer.readLongLE();
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
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                long l = buffer.readLongLE();
                                if (BlockUtil.isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    Instant instant = DateTimeUtils.parseTimestampColumnData(l, precision);
                                    col.add(instant);
                                }
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_FLOAT: {
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                float f = buffer.readFloatLE();
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
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                double d = buffer.readDoubleLE();
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
                        case TSDB_DATA_TYPE_GEOMETRY: {
                            length = numOfRows * 4;
                            List<Integer> offset = new ArrayList<>(numOfRows);
                            for (int m = 0; m < numOfRows; m++) {
                                offset.add(buffer.readIntLE());
                            }
                            int start = buffer.readerIndex();
                            for (int m = 0; m < numOfRows; m++) {
                                if (-1 == offset.get(m)) {
                                    col.add(null);
                                    continue;
                                }
                                buffer.readerIndex(start + offset.get(m));
                                int len;
                                if (type != TSDB_DATA_TYPE_BLOB) {
                                    len = buffer.readShortLE() & 0xFFFF;

                                } else {
                                    len = buffer.readIntLE();
                                }

                                byte[] tmp = new byte[len];
                                buffer.readBytes(tmp);
                                col.add(tmp);
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_NCHAR: {
                            length = numOfRows * 4;
                            List<Integer> offset = new ArrayList<>(numOfRows);
                            for (int m = 0; m < numOfRows; m++) {
                                offset.add(buffer.readIntLE());
                            }
                            int start = buffer.readerIndex();
                            for (int m = 0; m < numOfRows; m++) {
                                if (-1 == offset.get(m)) {
                                    col.add(null);
                                    continue;
                                }
                                buffer.readerIndex(start + offset.get(m));
                                int len = (buffer.readShortLE() & 0xFFFF) / 4;
                                int[] tmp = new int[len];
                                for (int n = 0; n < len; n++) {
                                    tmp[n] = buffer.readIntLE();
                                }
                                col.add(tmp);
                            }
                            break;
                        }
                        case TSDB_DATA_TYPE_DECIMAL128:
                        case TSDB_DATA_TYPE_DECIMAL64:
                            int dataLen = type == TSDB_DATA_TYPE_DECIMAL128 ? 16 : 8;
                            length = bitMapOffset;
                            byte[] tmp = new byte[bitMapOffset];
                            buffer.readBytes(tmp);
                            for (int j = 0; j < numOfRows; j++) {
                                byte[] tb = new byte[dataLen];
                                buffer.readBytes(tb);

                                if (BlockUtil.isNull(tmp, j)) {
                                    col.add(null);
                                } else {
                                    BigDecimal t = DecimalUtil.getBigDecimal(tb, scale);
                                    col.add(t);
                                }
                            }
                            break;
                        default:
                            // unknown type, do nothing
                            col.add(null);
                            break;
                    }
                    pHeader += length + lengths.get(i);
                    buffer.readerIndex(pHeader);
                    list.add(col);
                }
            }
            this.data = list;
            semaphore.release();
        } catch (Exception e){
            log.error("block data handle error: {}", e.getMessage());
        } finally {
            ReferenceCountUtil.safeRelease(buffer);
        }
    }

    private int bitmapLen(int n) {
        return (n + 0x7) >> 3;
    }

    public void doneWithNoData(){
        semaphore.release();
    }

    public void waitTillOK() throws SQLException {
        try {
            // must be ok When the CPU has idle time
            if (!semaphore.tryAcquire(50, TimeUnit.SECONDS))
            {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "FETCH DATA TIME OUT");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public List<List<Object>> getData() {
        return data;
    }

    public void setData(List<List<Object>> data) {
        this.data = data;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    public int getNumOfRows() {
        return numOfRows;
    }

    public void setNumOfRows(int numOfRows) {
        this.numOfRows = numOfRows;
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public List<RestfulResultSet.Field> getFields() {
        return fields;
    }

    public void setFields(List<RestfulResultSet.Field> fields) {
        this.fields = fields;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }
}
