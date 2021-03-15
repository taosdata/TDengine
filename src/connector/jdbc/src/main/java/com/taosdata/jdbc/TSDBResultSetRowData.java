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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;

public class TSDBResultSetRowData {
    private ArrayList<Object> data = null;
    private int colSize = 0;

    public TSDBResultSetRowData(int colSize) {
        this.setColSize(colSize);
    }

    public TSDBResultSetRowData() {
        this.data = new ArrayList<>();
        this.setColSize(0);
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
        return data.get(col) == null;
    }

    public void setBoolean(int col, boolean value) {
        data.set(col, value);
    }

    public boolean getBoolean(int col, int srcType) throws SQLException {
        Object obj = data.get(col);

        switch (srcType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return (Boolean) obj;
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return ((Float) obj) == 1.0 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return ((Double) obj) == 1.0 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return ((Byte) obj) == 1 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return ((Short) obj) == 1 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return ((Integer) obj) == 1 ? Boolean.TRUE : Boolean.FALSE;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return ((Long) obj) == 1L ? Boolean.TRUE : Boolean.FALSE;
        }

        return Boolean.TRUE;
    }

    public void setByte(int col, byte value) {
        data.set(col, value);
    }

    public void setShort(int col, short value) {
        data.set(col, value);
    }

    public void setInt(int col, int value) {
        data.set(col, value);
    }

    public int getInt(int col, int srcType) throws SQLException {
        Object obj = data.get(col);

        switch (srcType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return Boolean.TRUE.equals(obj) ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return ((Float) obj).intValue();
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return ((Double) obj).intValue();
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (Byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (Short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return (Integer) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return ((Long) obj).intValue();
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return Integer.parseInt((String) obj);
        }

        return 0;
    }

    public void setLong(int col, long value) {
        data.set(col, value);
    }

    public long getLong(int col, int srcType) throws SQLException {
        Object obj = data.get(col);

        switch (srcType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return Boolean.TRUE.equals(obj) ? 1 : 0;
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return ((Float) obj).longValue();
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return ((Double) obj).longValue();
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return (Byte) obj;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return (Short) obj;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return (Integer) obj;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return (Long) obj;
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return Long.parseLong((String) obj);
        }

        return 0;
    }

    public void setFloat(int col, float value) {
        data.set(col, value);
    }

    public float getFloat(int col, int srcType) throws SQLException {
        Object obj = data.get(col);

        switch (srcType) {
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
        }

        return 0;
    }

    public void setDouble(int col, double value) {
        data.set(col, value);
    }

    public double getDouble(int col, int srcType) throws SQLException {
        Object obj = data.get(col);

        switch (srcType) {
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
        }

        return 0;
    }

    public void setString(int col, String value) {
        data.set(col, value);
    }

    public void setByteArray(int col, byte[] value) {
        try {
            data.set(col, new String(value, TaosGlobalConfig.getCharset()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * The original type may not be a string type, but will be converted to by calling this method
     *
     * @param col column index
     * @return
     * @throws SQLException
     */
    public String getString(int col, int srcType) throws SQLException {
        switch (srcType) {
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                return (String) data.get(col);
            case TSDBConstants.TSDB_DATA_TYPE_UTINYINT: {
                Byte value = new Byte(String.valueOf(data.get(col)));
                if (value >= 0)
                    return value.toString();
                return Integer.toString(value & 0xff);
            }
            case TSDBConstants.TSDB_DATA_TYPE_USMALLINT: {
                Short value = new Short(String.valueOf(data.get(col)));
                if (value >= 0)
                    return value.toString();
                return Integer.toString(value & 0xffff);
            }
            case TSDBConstants.TSDB_DATA_TYPE_UINT: {
                Integer value = new Integer(String.valueOf(data.get(col)));
                if (value >= 0)
                    return value.toString();
                return Long.toString(value & 0xffffffffl);
            }
            case TSDBConstants.TSDB_DATA_TYPE_UBIGINT: {
                Long value = new Long(String.valueOf(data.get(col)));
                if (value >= 0)
                    return value.toString();
                long lowValue = value & 0x7fffffffffffffffL;
                return BigDecimal.valueOf(lowValue).add(BigDecimal.valueOf(Long.MAX_VALUE)).add(BigDecimal.valueOf(1)).toString();
            }
            default:
                return String.valueOf(data.get(col));
        }
    }

    public void setTimestamp(int col, long ts) {
        data.set(col, ts);
    }

    public Timestamp getTimestamp(int col) {
        return new Timestamp((Long) data.get(col));
    }

    public Object get(int col) {
        return data.get(col);
    }

    public int getColSize() {
        return colSize;
    }

    public void setColSize(int colSize) {
        this.colSize = colSize;
        this.clear();
    }

    public ArrayList<Object> getData() {
        return data;
    }

    public void setData(ArrayList<Object> data) {
        this.data = (ArrayList<Object>) data.clone();
    }
}
