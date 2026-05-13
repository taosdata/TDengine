package com.taosdata.jdbc;

import java.sql.SQLException;
import java.sql.Types;

public abstract class TSDBConstants {

    public static final long JNI_NULL_POINTER = 0L;
    // JNI_ERROR_NUMBER
    public static final int JNI_SUCCESS = 0;
    public static final int JNI_TDENGINE_ERROR = -1;
    public static final int JNI_CONNECTION_NULL = -2;
    public static final int JNI_RESULT_SET_NULL = -3;
    public static final int JNI_NUM_OF_FIELDS_0 = -4;
    public static final int JNI_SQL_NULL = -5;
    public static final int JNI_FETCH_END = -6;
    public static final int JNI_OUT_OF_MEMORY = -7;

    // TMQ
    public static final int TMQ_SUCCESS = JNI_SUCCESS;
    public static final int TMQ_CONF_NULL = -100;
    public static final int TMQ_CONF_KEY_NULL = -101;
    public static final int TMQ_CONF_VALUE_NULL = -102;
    public static final int TMQ_TOPIC_NULL = -110;
    public static final int TMQ_TOPIC_NAME_NULL = -111;
    public static final int TMQ_CONSUMER_NULL = -120;
    public static final int TMQ_CONSUMER_CREATE_ERROR = -121;

    // TSDB Error Code
    public static final int  TIMESTAMP_DATA_OUT_OF_RANGE  = 0x60b;

    // TSDB Data Types
    public static final int TSDB_DATA_TYPE_NULL = 0;
    public static final int TSDB_DATA_TYPE_BOOL = 1;
    public static final int TSDB_DATA_TYPE_TINYINT = 2;
    public static final int TSDB_DATA_TYPE_SMALLINT = 3;
    public static final int TSDB_DATA_TYPE_INT = 4;
    public static final int TSDB_DATA_TYPE_BIGINT = 5;
    public static final int TSDB_DATA_TYPE_FLOAT = 6;
    public static final int TSDB_DATA_TYPE_DOUBLE = 7;
    public static final int TSDB_DATA_TYPE_VARCHAR = 8;
    public static final int TSDB_DATA_TYPE_BINARY = TSDB_DATA_TYPE_VARCHAR;
    public static final int TSDB_DATA_TYPE_TIMESTAMP = 9;
    public static final int TSDB_DATA_TYPE_NCHAR = 10;
    /**
     * 系统增加新的无符号数据类型，分别是：
     * unsigned tinyint， 数值范围：0-255
     * unsigned smallint，数值范围： 0-65535
     * unsigned int，数值范围：0-4294967295
     * unsigned bigint，数值范围：0-18446744073709551615u。
     * example:
     * create table tb(ts timestamp, a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned);
     */
    public static final int TSDB_DATA_TYPE_UTINYINT = 11;       //unsigned tinyint
    public static final int TSDB_DATA_TYPE_USMALLINT = 12;      //unsigned smallint
    public static final int TSDB_DATA_TYPE_UINT = 13;           //unsigned int
    public static final int TSDB_DATA_TYPE_UBIGINT = 14;        //unsigned bigint
    public static final int TSDB_DATA_TYPE_JSON = 15;           //json
    public static final int TSDB_DATA_TYPE_VARBINARY = 16;     //varbinary
    public static final int TSDB_DATA_TYPE_DECIMAL128 = 17;     //decimal128
    public static final int TSDB_DATA_TYPE_BLOB = 18;     //blob
    public static final int TSDB_DATA_TYPE_MEDIUMBLOB = 19;     //
    public static final int TSDB_DATA_TYPE_GEOMETRY = 20;     //geometry
    public static final int TSDB_DATA_TYPE_DECIMAL64 = 21;     //decimal64

    // nchar column max length
    public static final int MAX_FIELD_SIZE = 16 * 1024;

    // precision for data types, this is used for metadata
    public static final int BOOLEAN_PRECISION = 1;
    public static final int TINYINT_PRECISION = 3;
    public static final int UNSIGNED_TINYINT_PRECISION = 3;
    public static final int SMALLINT_PRECISION = 5;
    public static final int UNSIGNED_SMALLINT_PRECISION = 5;

    public static final int INT_PRECISION = 10;
    public static final int UNSIGNED_INT_PRECISION = 10;
    public static final int BIGINT_PRECISION = 19;
    public static final int UNSIGNED_BIGINT_PRECISION = 20;
    public static final int FLOAT_PRECISION = 12;
    public static final int DOUBLE_PRECISION = 22;
    public static final int TIMESTAMP_MS_PRECISION = 23;
    public static final int TIMESTAMP_US_PRECISION = 26;

    public static final int DECIMAL128_PRECISION = 38;
    public static final int DECIMAL64_PRECISION = 18;

    // scale for data types, this is used for metadata
    public static final int FLOAT_SCALE = 31;
    public static final int DOUBLE_SCALE = 31;

    public static final String DEFAULT_PRECISION = "ms";
    public static final int DEFAULT_MESSAGE_WAIT_TIMEOUT = 60_000;
    public static final boolean DEFAULT_BATCH_ERROR_IGNORE = false;

    public static final short MAX_UNSIGNED_BYTE = 255;
    public static final int MAX_UNSIGNED_SHORT = 65535;
    public static final long MAX_UNSIGNED_INT = 4294967295L;
    public static final String MAX_UNSIGNED_LONG = "18446744073709551615";
    public static final String MIN_SUPPORT_VERSION = "3.3.6.0";  // NOSONAR
    public static final String MIN_BLOB_SUPPORT_VERSION = "3.3.7.0.alpha";
    public static final String UNKNOWN_VERSION = "unknown";

    public static String jdbcType2TaosTypeName(int jdbcType) throws SQLException {
        switch (jdbcType) {
            case Types.BOOLEAN:
                return "BOOL";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.BINARY:
                return "BINARY";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.NCHAR:
                return "NCHAR";
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE, "unknown sql type: " + jdbcType + " in tdengine");
        }
    }

}
