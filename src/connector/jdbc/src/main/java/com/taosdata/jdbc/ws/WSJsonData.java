package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBConstants;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class WSJsonData {
    private List<Object> data;

    public WSJsonData(Object[] data) {
        this.data = Arrays.asList(data);
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
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
                return obj.toString().contains("1");
            }
            default:
                return false;
        }
    }

    public String getString(int col, int nativeType){
        Object obj = data.get(col -1 );
        if (null != obj){
            return null;
        }
        switch (nativeType){
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
            case TSDBConstants.TSDB_DATA_TYPE_JSON:
                return (String) obj;
            default:
                return String.valueOf(obj);
        }
    }
}
