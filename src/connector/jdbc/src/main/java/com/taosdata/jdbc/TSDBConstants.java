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

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public abstract class TSDBConstants {

    public static final String DEFAULT_PORT = "6200";
    public static Map<Integer, String> DATATYPE_MAP = null;

    public static final long JNI_NULL_POINTER = 0L;

    public static final int JNI_SUCCESS = 0;
    public static final int JNI_TDENGINE_ERROR = -1;
    public static final int JNI_CONNECTION_NULL = -2;
    public static final int JNI_RESULT_SET_NULL = -3;
    public static final int JNI_NUM_OF_FIELDS_0 = -4;
    public static final int JNI_SQL_NULL = -5;
    public static final int JNI_FETCH_END = -6;
    public static final int JNI_OUT_OF_MEMORY = -7;

    public static final int TSDB_DATA_TYPE_NULL = 0;
    public static final int TSDB_DATA_TYPE_BOOL = 1;
    public static final int TSDB_DATA_TYPE_TINYINT = 2;
    public static final int TSDB_DATA_TYPE_SMALLINT = 3;
    public static final int TSDB_DATA_TYPE_INT = 4;
    public static final int TSDB_DATA_TYPE_BIGINT = 5;
    public static final int TSDB_DATA_TYPE_FLOAT = 6;
    public static final int TSDB_DATA_TYPE_DOUBLE = 7;
    public static final int TSDB_DATA_TYPE_BINARY = 8;
    public static final int TSDB_DATA_TYPE_TIMESTAMP = 9;
    public static final int TSDB_DATA_TYPE_NCHAR = 10;

    // nchar field's max length
    public static final int maxFieldSize = 16 * 1024;

    public static String WrapErrMsg(String msg) {
        return "TDengine Error: " + msg;
    }

    public static String FixErrMsg(int code) {
        switch (code) {
            case JNI_TDENGINE_ERROR:
                return WrapErrMsg("internal error of database!");
            case JNI_CONNECTION_NULL:
                return WrapErrMsg("invalid tdengine connection!");
            case JNI_RESULT_SET_NULL:
                return WrapErrMsg("invalid resultset pointer!");
            case JNI_NUM_OF_FIELDS_0:
                return WrapErrMsg("invalid num of fields!");
            case JNI_SQL_NULL:
                return WrapErrMsg("can't execute empty sql!");
            case JNI_FETCH_END:
                return WrapErrMsg("fetch to the end of resultset");
            default:
                break;
        }
        return WrapErrMsg("unkown error!");
    }

    public static int taosType2JdbcType(int taosType) throws SQLException {
        switch (taosType) {
            case TSDBConstants.TSDB_DATA_TYPE_NULL:
                return Types.NULL;
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return Types.BOOLEAN;
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return Types.TINYINT;
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return Types.SMALLINT;
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return Types.INTEGER;
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return Types.BIGINT;
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return Types.FLOAT;
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return Types.DOUBLE;
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return Types.BINARY;
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
                return Types.TIMESTAMP;
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                return Types.NCHAR;
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
    }

    public static int jdbcType2TaosType(int jdbcType) throws SQLException {
        switch (jdbcType){
            case Types.NULL:
                return TSDBConstants.TSDB_DATA_TYPE_NULL;
            case Types.BOOLEAN:
                return TSDBConstants.TSDB_DATA_TYPE_BOOL;
            case Types.TINYINT:
                return TSDBConstants.TSDB_DATA_TYPE_TINYINT;
            case Types.SMALLINT:
                return TSDBConstants.TSDB_DATA_TYPE_SMALLINT;
            case Types.INTEGER:
                return TSDBConstants.TSDB_DATA_TYPE_INT;
            case Types.BIGINT:
                return TSDBConstants.TSDB_DATA_TYPE_BIGINT;
            case Types.FLOAT:
                return TSDBConstants.TSDB_DATA_TYPE_FLOAT;
            case Types.DOUBLE:
                return TSDBConstants.TSDB_DATA_TYPE_DOUBLE;
            case Types.BINARY:
                return TSDBConstants.TSDB_DATA_TYPE_BINARY;
            case Types.TIMESTAMP:
                return TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP;
            case Types.NCHAR:
                return TSDBConstants.TSDB_DATA_TYPE_NCHAR;
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
    }

    static {
        DATATYPE_MAP = new HashMap<>();
        DATATYPE_MAP.put(0, "NULL");
        DATATYPE_MAP.put(1, "BOOL");
        DATATYPE_MAP.put(2, "TINYINT");
        DATATYPE_MAP.put(3, "SMALLINT");
        DATATYPE_MAP.put(4, "INT");
        DATATYPE_MAP.put(5, "BIGINT");
        DATATYPE_MAP.put(6, "FLOAT");
        DATATYPE_MAP.put(7, "DOUBLE");
        DATATYPE_MAP.put(8, "BINARY");
        DATATYPE_MAP.put(9, "TIMESTAMP");
        DATATYPE_MAP.put(10, "NCHAR");
    }

    public static String jdbcType2TaosTypeName(int type) throws SQLException {
        return DATATYPE_MAP.get(jdbcType2TaosType(type));
    }
}
