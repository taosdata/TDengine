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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.utils.NullType;

public class TSDBResultSetBlockData {
    private static final int BINARY_LENGTH_OFFSET = 2;
    private int numOfRows = 0;
    private int rowIndex = 0;

    private List<ColumnMetaData> columnMetaDataList;
    private ArrayList<Object> colData;

    private int timestampPrecision;

    public TSDBResultSetBlockData(List<ColumnMetaData> colMeta, int numOfCols, int timestampPrecision) {
        this.columnMetaDataList = colMeta;
        this.colData = new ArrayList<>(numOfCols);
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
        this.colData.addAll(Collections.nCopies(numOfCols, null));
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

    public void setBoolean(int col, boolean value) {
        colData.set(col, value);
    }

    public void setByteArray(int col, int length, byte[] value) {
        switch (this.columnMetaDataList.get(col).getColType()) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                buf.order(ByteOrder.LITTLE_ENDIAN).asCharBuffer();
                this.colData.set(col, buf);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_UTINYINT:
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                this.colData.set(col, buf);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_USMALLINT:
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                ShortBuffer sb = buf.order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
                this.colData.set(col, sb);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_UINT:
            case TSDBConstants.TSDB_DATA_TYPE_INT: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                IntBuffer ib = buf.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
                this.colData.set(col, ib);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_UBIGINT:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                LongBuffer lb = buf.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
                this.colData.set(col, lb);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                FloatBuffer fb = buf.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
                this.colData.set(col, fb);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                DoubleBuffer db = buf.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
                this.colData.set(col, db);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                this.colData.set(col, buf);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                LongBuffer lb = buf.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
                this.colData.set(col, lb);
                break;
            }
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
                ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                this.colData.set(col, buf);
                break;
            }
        }
    }


    /**
     * The original type may not be a string type, but will be converted to by
     * calling this method
     */
    public String getString(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
//            return new NullType().toString();
            return null;
        }

        if (obj instanceof String)
            return (String) obj;

        if (obj instanceof byte[]) {
            String charset = TaosGlobalConfig.getCharset();
            try {
                return new String((byte[]) obj, charset);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return obj.toString();
    }

    public byte[] getBytes(int col) throws SQLException {

        Object obj = get(col);
        if (obj == null) {
            return null;
        }
        if (obj instanceof byte[])
            return (byte[]) obj;
        if (obj instanceof String)
            return ((String) obj).getBytes();
        if (obj instanceof Long)
            return Longs.toByteArray((long) obj);
        if (obj instanceof Integer)
            return Ints.toByteArray((int) obj);
        if (obj instanceof Short)
            return Shorts.toByteArray((short) obj);
        if (obj instanceof Byte)
            return new byte[]{(byte) obj};

        return obj.toString().getBytes();
    }

    public int getInt(int col) {
        Object obj = get(col);
        if (obj == null) {
            return 0;
        }

        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return (boolean) obj ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT: {
                return (int) obj;
            }
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return ((Long) obj).intValue();
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                return ((Long) ((Timestamp) obj).getTime()).intValue();
            }

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                return ((Double) obj).intValue();
            }

            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
                return Integer.parseInt((String) obj);
            }
        }

        return 0;
    }

    public boolean getBoolean(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            return Boolean.FALSE;
        }

        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return (boolean) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return ((byte) obj == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return ((short) obj == 0) ? Boolean.FALSE : Boolean.TRUE;
            case TSDBConstants.TSDB_DATA_TYPE_INT: {
                return ((int) obj == 0) ? Boolean.FALSE : Boolean.TRUE;
            }
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return (((long) obj) == 0L) ? Boolean.FALSE : Boolean.TRUE;

            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                return ((Timestamp) obj).getTime() == 0L ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return (((float) obj) == 0f) ? Boolean.FALSE : Boolean.TRUE;
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                return (((double) obj) == 0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
                if ("TRUE".compareToIgnoreCase((String) obj) == 0) {
                    return Boolean.TRUE;
                } else if ("FALSE".compareToIgnoreCase((String) obj) == 0) {
                    return Boolean.TRUE;
                } else {
                    throw new SQLDataException();
                }
            }
        }

        return Boolean.FALSE;
    }

    public long getLong(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            return 0;
        }

        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return (boolean) obj ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT: {
                return (int) obj;
            }
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return (long) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                Timestamp ts = (Timestamp) obj;
                switch (this.timestampPrecision) {
                    case TimestampPrecision.MS:
                    default:
                        return ts.getTime();
                    case TimestampPrecision.US:
                        return ts.getTime() * 1000 + ts.getNanos() / 1000 % 1000;
                    case TimestampPrecision.NS:
                        return ts.getTime() * 1000_000 + ts.getNanos() % 1000_000;
                }
            }

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return ((Float) obj).longValue();
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                return ((Double) obj).longValue();
            }

            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
                return Long.parseLong((String) obj);
            }
        }

        return 0;
    }

    public Timestamp getTimestamp(int col) throws SQLException {
        Object obj = get(col);
        if (obj == null) {
            return null;
        }

        int type = this.columnMetaDataList.get(col).getColType();
        if (type == TSDBConstants.TSDB_DATA_TYPE_BIGINT)
            return parseTimestampColumnData((long) obj);
        if (type == TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP)
            return (Timestamp) obj;

        return new Timestamp(getLong(col));
    }

    public double getDouble(int col) {
        Object obj = get(col);
        if (obj == null) {
            return 0;
        }

        int type = this.columnMetaDataList.get(col).getColType();
        switch (type) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return (boolean) obj ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT: {
                return (int) obj;
            }
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return (long) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                Timestamp ts = (Timestamp) obj;
                switch (this.timestampPrecision) {
                    case TimestampPrecision.MS:
                    default:
                        return ts.getTime();
                    case TimestampPrecision.US:
                        return ts.getTime() * 1000 + ts.getNanos() / 1000 % 1000;
                    case TimestampPrecision.NS:
                        return ts.getTime() * 1000_000 + ts.getNanos() % 1000_000;
                }
            }

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return Double.parseDouble(String.valueOf(obj));
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                return (double) obj;
            }

            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
                return Double.parseDouble((String) obj);
            }
        }

        return 0;
    }

    public Object get(int col) {
        int fieldSize = this.columnMetaDataList.get(col).getColSize();

        switch (this.columnMetaDataList.get(col).getColType()) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL: {
                ByteBuffer bb = (ByteBuffer) this.colData.get(col);

                byte val = bb.get(this.rowIndex);
                if (NullType.isBooleanNull(val)) {
                    return null;
                }

                return (val == 0x0) ? Boolean.FALSE : Boolean.TRUE;
            }

            case TSDBConstants.TSDB_DATA_TYPE_TINYINT: {
                ByteBuffer bb = (ByteBuffer) this.colData.get(col);

                byte val = bb.get(this.rowIndex);
                if (NullType.isTinyIntNull(val)) {
                    return null;
                }

                return val;
            }

            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT: {
                ShortBuffer sb = (ShortBuffer) this.colData.get(col);
                short val = sb.get(this.rowIndex);
                if (NullType.isSmallIntNull(val)) {
                    return null;
                }

                return val;
            }

            case TSDBConstants.TSDB_DATA_TYPE_INT: {
                IntBuffer ib = (IntBuffer) this.colData.get(col);
                int val = ib.get(this.rowIndex);
                if (NullType.isIntNull(val)) {
                    return null;
                }

                return val;
            }

            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                LongBuffer lb = (LongBuffer) this.colData.get(col);
                long val = lb.get(this.rowIndex);
                if (NullType.isBigIntNull(val)) {
                    return null;
                }

                return parseTimestampColumnData(val);
            }
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT: {
                LongBuffer lb = (LongBuffer) this.colData.get(col);
                long val = lb.get(this.rowIndex);
                if (NullType.isBigIntNull(val)) {
                    return null;
                }

                return val;
            }

            case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
                FloatBuffer fb = (FloatBuffer) this.colData.get(col);
                float val = fb.get(this.rowIndex);
                if (NullType.isFloatNull(val)) {
                    return null;
                }

                return val;
            }

            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
                DoubleBuffer lb = (DoubleBuffer) this.colData.get(col);
                double val = lb.get(this.rowIndex);
                if (NullType.isDoubleNull(val)) {
                    return null;
                }

                return val;
            }

            case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
                ByteBuffer bb = (ByteBuffer) this.colData.get(col);
                bb.position((fieldSize + BINARY_LENGTH_OFFSET) * this.rowIndex);
                int length = bb.getShort();
                byte[] dest = new byte[length];
                bb.get(dest, 0, length);
                if (NullType.isBinaryNull(dest, length)) {
                    return null;
                }

                return dest;
            }

            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
                ByteBuffer bb = (ByteBuffer) this.colData.get(col);
//                bb.position((fieldSize * 4 + 2 + 1) * this.rowIndex);
                bb.position(bb.capacity() / numOfRows * this.rowIndex);
                int length = bb.getShort();
                byte[] dest = new byte[length];
                bb.get(dest, 0, length);
                if (NullType.isNcharNull(dest, length)) {
                    return null;
                }
                try {
                    String charset = TaosGlobalConfig.getCharset();
                    return new String(dest, charset);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        return 0;
    }

    private Timestamp parseTimestampColumnData(long value) {
        if (TimestampPrecision.MS == timestampPrecision)
            return new Timestamp(value);

        if (TimestampPrecision.US == timestampPrecision) {
            long epochSec = value / 1000_000L;
            long nanoAdjustment = value % 1000_000L * 1000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        if (TimestampPrecision.NS == timestampPrecision) {
            long epochSec = value / 1000_000_000L;
            long nanoAdjustment = value % 1000_000_000L;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        return null;
    }
}
