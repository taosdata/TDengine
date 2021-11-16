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

import com.taosdata.jdbc.utils.NullType;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
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

        switch (nativeType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return (Boolean) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return ((Byte) obj) == 1 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return ((Short) obj) == 1 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return ((Integer) obj) == 1 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return ((Long) obj) == 1L ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
                return obj.toString().contains("1");
            }
            default:
                return false;
        }
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
            return NullType.getIntNull();

        switch (nativeType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return Boolean.TRUE.equals(obj) ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (Byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (Short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return (Integer) obj;
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                return ((Long) obj).intValue();
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return Integer.parseInt((String) obj);
            case TSDBConstants.TSDB_DATA_TYPE_UTINYINT:
                return parseUnsignedTinyIntToInt(obj);
            case TSDBConstants.TSDB_DATA_TYPE_USMALLINT:
                return parseUnsignedSmallIntToInt(obj);
            case TSDBConstants.TSDB_DATA_TYPE_UINT:
                return parseUnsignedIntegerToInt(obj);
            case TSDBConstants.TSDB_DATA_TYPE_UBIGINT:
                return parseUnsignedBigIntToInt(obj);
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return ((Float) obj).intValue();
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return ((Double) obj).intValue();
            default:
                return 0;
        }
    }

    private byte parseUnsignedTinyIntToInt(Object obj) throws SQLException {
        byte value = (byte) obj;
        if (value < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
        return value;
    }

    private short parseUnsignedSmallIntToInt(Object obj) throws SQLException {
        short value = (short) obj;
        if (value < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
        return value;
    }

    private int parseUnsignedIntegerToInt(Object obj) throws SQLException {
        int value = (int) obj;
        if (value < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
        return value;
    }

    private int parseUnsignedBigIntToInt(Object obj) throws SQLException {
        long value = (long) obj;
        if (value < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
        return (int) value;
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
            return NullType.getBigIntNull();
        }

        switch (nativeType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return Boolean.TRUE.equals(obj) ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (Byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (Short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return (Integer) obj;
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                return (Long) obj;
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return Long.parseLong((String) obj);
            case TSDBConstants.TSDB_DATA_TYPE_UTINYINT: {
                byte value = (byte) obj;
                if (value < 0)
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
                return value;
            }
            case TSDBConstants.TSDB_DATA_TYPE_USMALLINT: {
                short value = (short) obj;
                if (value < 0)
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
                return value;
            }
            case TSDBConstants.TSDB_DATA_TYPE_UINT: {
                int value = (int) obj;
                if (value < 0)
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
                return value;
            }
            case TSDBConstants.TSDB_DATA_TYPE_UBIGINT: {
                long value = (long) obj;
                if (value < 0)
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
                return value;
            }
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return ((Float) obj).longValue();
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return ((Double) obj).longValue();
            default:
                return 0;
        }
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

    public float getFloat(int col, int nativeType) {
        Object obj = data.get(col - 1);
        if (obj == null)
            return NullType.getFloatNull();

        switch (nativeType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return Boolean.TRUE.equals(obj) ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return (Float) obj;
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return ((Double) obj).floatValue();
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (Byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (Short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return (Integer) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return (Long) obj;
            default:
                return NullType.getFloatNull();
        }
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

    public double getDouble(int col, int nativeType) {
        Object obj = data.get(col - 1);
        if (obj == null)
            return NullType.getDoubleNull();

        switch (nativeType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return Boolean.TRUE.equals(obj) ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return (Float) obj;
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return (Double) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (Byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (Short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return (Integer) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return (Long) obj;
            default:
                return NullType.getDoubleNull();
        }
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
        // TODO:
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
        // TODO:
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

    public String getString(int col, int nativeType) {
        Object obj = data.get(col - 1);
        if (obj == null)
            return null;

        switch (nativeType) {
            case TSDBConstants.TSDB_DATA_TYPE_UTINYINT: {
                byte value = new Byte(String.valueOf(obj));
                if (value >= 0)
                    return Byte.toString(value);
                return Integer.toString(value & 0xff);
            }
            case TSDBConstants.TSDB_DATA_TYPE_USMALLINT: {
                short value = new Short(String.valueOf(obj));
                if (value >= 0)
                    return Short.toString(value);
                return Integer.toString(value & 0xffff);
            }
            case TSDBConstants.TSDB_DATA_TYPE_UINT: {
                int value = new Integer(String.valueOf(obj));
                if (value >= 0)
                    return Integer.toString(value);
                return Long.toString(value & 0xffffffffL);
            }
            case TSDBConstants.TSDB_DATA_TYPE_UBIGINT: {
                long value = new Long(String.valueOf(obj));
                if (value >= 0)
                    return Long.toString(value);
                long lowValue = value & 0x7fffffffffffffffL;
                return BigDecimal.valueOf(lowValue).add(BigDecimal.valueOf(Long.MAX_VALUE)).add(BigDecimal.valueOf(1)).toString();
            }
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return new String((byte[]) obj);
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                return (String) obj;
            default:
                return String.valueOf(obj);
        }
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
                break;
            }
            case 1: {
                milliseconds = ts / 1_000;
                fracNanoseconds = (int) (ts * 1_000 % 1_000_000_000);
                break;
            }
            case 2: {
                milliseconds = ts / 1_000_000;
                fracNanoseconds = (int) (ts % 1_000_000_000);
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
        //TODO: this implementation contains logical error
        // when precision is us the (long ts) is 16 digital number
        // when precision is ms, the (long ts) is 13 digital number
        // we need a JNI function like this:
        //      public void setTimestamp(int col, long epochSecond, long nanoAdjustment)
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
