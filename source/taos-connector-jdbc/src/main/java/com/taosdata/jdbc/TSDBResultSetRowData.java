package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.TimestampPrecision;
import com.taosdata.jdbc.utils.DataTypeConvertUtil;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;

public class TSDBResultSetRowData {

    private ArrayList<Object> data;
    private final int colSize;

    public TSDBResultSetRowData(int colSize) {
        this.colSize = colSize;
        this.clear();
    }

    public void clear() {
        if (this.data != null) {
            this.data.clear();
        }
        if (this.colSize == 0) {
            return;
        }
        this.data = new ArrayList<>(colSize);
        this.data.addAll(Collections.nCopies(this.colSize, null));
    }

    public boolean wasNull(int col) {
        return data.get(col - 1) == null;
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setBooleanValue(int col, boolean value) {
        setBoolean(col - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setBoolean(int col, boolean value) {
        data.set(col, value);
    }

    public boolean getBoolean(int col, int nativeType) throws SQLException {
        Object obj = data.get(col - 1);
        if (obj instanceof Boolean)
            return (boolean) obj;

        return DataTypeConvertUtil.getBoolean(nativeType, obj);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setByteValue(int colIndex, byte value) {
        setByte(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setByte(int col, byte value) {
        data.set(col, value);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setShortValue(int colIndex, short value) {
        setShort(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setShort(int col, short value) {
        data.set(col, value);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setIntValue(int colIndex, int value) {
        setInt(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setInt(int col, int value) {
        data.set(col, value);
    }

    public int getInt(int col, int nativeType) throws SQLException {
        Object obj = data.get(col - 1);
        if (obj == null)
            return 0;

        return DataTypeConvertUtil.getInt(nativeType, obj, col);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setLongValue(int colIndex, long value) {
        setLong(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setLong(int col, long value) {
        data.set(col, value);
    }

    public long getLong(int col, int nativeType) throws SQLException {
        Object obj = data.get(col - 1);
        if (obj == null) {
            return 0;
        }

        if (obj instanceof Long) {
            return (long) obj;
        }

        return DataTypeConvertUtil.getLong(nativeType, obj, col, TimestampPrecision.MS);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setFloatValue(int colIndex, float value) {
        setFloat(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setFloat(int col, float value) {
        data.set(col, value);
    }

    public float getFloat(int col, int nativeType) throws SQLException {
        Object obj = data.get(col - 1);
        if (obj == null)
            return 0;
        if (obj instanceof Float)
            return (float) obj;
        return DataTypeConvertUtil.getFloat(nativeType, obj, col);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setDoubleValue(int colIndex, double value) {
        setDouble(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setDouble(int col, double value) {
        data.set(col, value);
    }

    public double getDouble(int col, int nativeType) throws SQLException {
        Object obj = data.get(col - 1);
        if (obj == null)
            return 0;
        return DataTypeConvertUtil.getDouble(nativeType, obj, col, TimestampPrecision.MS);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setStringValue(int colIndex, String value) {
        data.set(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setString(int col, String value) {
        // TODO: // NOSONAR
        //  !!!NOTE!!!
        //  this is very confusing problem which related to JNI-method implementation,
        //  the JNI method return a String(encoded in UTF) for BINARY value, which means the JNI method will invoke
        //  this setString(int, String) to handle BINARY value, we need to build a byte[] with default charsetEncoding
        data.set(col, value == null ? null : value.getBytes());
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setByteArrayValue(int colIndex, byte[] value) {
        setByteArray(colIndex - 1, value);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     */
    public void setByteArray(int col, byte[] value) {
        // TODO: // NOSONAR
        //  !!!NOTE!!!
        //  this is very confusing problem which related to JNI-method implementation,
        //  the JNI method return a byte[] for NCHAR value, which means the JNI method will invoke
        //  this setByteArr(int, byte[]) to handle NCHAR value, we need to build a String with charsetEncoding by TaosGlobalConfig
        try {
            data.set(col, new String(value, TaosGlobalConfig.getCharset()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public String getString(int col, int nativeType) throws SQLException {
        Object obj = data.get(col - 1);
        if (obj == null)
            return null;

        return DataTypeConvertUtil.getString(obj);
    }

    /**
     * $$$ this method is invoked by databaseMetaDataResultSet and so on which use an index start from 1 in JDBC api
     */
    public void setTimestampValue(int colIndex, long value) {
        setTimestamp(colIndex - 1, value, 0);
    }

    /**
     * !!! this method is invoked by JNI method and the index start from 0 in C implementations
     *
     * @param precision 0 : ms, 1 : us, 2 : ns
     */
    public void setTimestamp(int col, long ts, int precision) {
        long milliseconds;
        int fracNanoseconds;
        switch (precision) {
            case 0: {
                milliseconds = ts;
                fracNanoseconds = (int) (ts * 1_000_000 % 1_000_000_000);
                fracNanoseconds = fracNanoseconds < 0 ? 1_000_000_000 + fracNanoseconds : fracNanoseconds;
                break;
            }
            case 1: {
                milliseconds = ts / 1_000;
                fracNanoseconds = (int) (ts * 1_000 % 1_000_000_000);
                if (fracNanoseconds < 0) {
                    if (milliseconds == 0) {
                        milliseconds = -1;
                    }
                    fracNanoseconds += 1_000_000_000;
                }
                break;
            }
            case 2: {
                milliseconds = ts / 1_000_000;
                fracNanoseconds = (int) (ts % 1_000_000_000);
                if (fracNanoseconds < 0) {
                    if (milliseconds == 0) {
                        milliseconds = -1;
                    }
                    fracNanoseconds += 1_000_000_000;
                }
                break;
            }
            default: {
                throw new IllegalArgumentException("precision is not valid. precision: " + precision);
            }
        }

        Timestamp tsObj = new Timestamp(milliseconds);
        tsObj.setNanos(fracNanoseconds);
        data.set(col, tsObj);
    }

    /**
     * this implementation is used for TDengine old version
     */
    public void setTimestamp(int col, long ts) {
        //TODO: this implementation contains logical error // NOSONAR
        // when precision is us the (long ts) is 16 digital number
        // when precision is ms, the (long ts) is 13 digital number
        // we need a JNI function like this:
        //      public void setTimestamp(int col, long epochSecond, long nanoAdjustment) // NOSONAR
        if (ts < 1_0000_0000_0000_0L) {
            data.set(col, new Timestamp(ts));
        } else {
            long epochSec = ts / 1000_000L;
            long nanoAdjustment = ts % 1000_000L * 1000L;
            Timestamp timestamp = Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
            data.set(col, timestamp);
        }
    }

    public Timestamp getTimestamp(int col, int nativeType) {
        Object obj = data.get(col - 1);
        if (obj == null)
            return null;
        if (nativeType == TSDBConstants.TSDB_DATA_TYPE_BIGINT) {
            return new Timestamp((Long) obj);
        }
        return (Timestamp) obj;
    }

    public Object getObject(int col) {
        return data.get(col - 1);
    }

}
