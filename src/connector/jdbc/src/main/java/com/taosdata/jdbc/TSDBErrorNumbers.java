package com.taosdata.jdbc;

import java.util.HashSet;
import java.util.Set;

public class TSDBErrorNumbers {

    public static final int ERROR_CONNECTION_CLOSED = 0x2301;   // connection already closed
    public static final int ERROR_UNSUPPORTED_METHOD = 0x2302;  //this operation is NOT supported currently!
    public static final int ERROR_INVALID_VARIABLE = 0x2303;    //invalid variables
    public static final int ERROR_STATEMENT_CLOSED = 0x2304;    //statement already closed
    public static final int ERROR_RESULTSET_CLOSED = 0x2305;    //resultSet is closed
    public static final int ERROR_BATCH_IS_EMPTY = 0x2306;      //Batch is empty!
    public static final int ERROR_INVALID_WITH_EXECUTEQUERY = 0x2307;  //Can not issue data manipulation statements with executeQuery()
    public static final int ERROR_INVALID_WITH_EXECUTEUPDATE = 0x2308; //Can not issue SELECT via executeUpdate()
    public static final int ERROR_INVALID_FOR_EXECUTE_QUERY = 0x2309;  //not a valid sql for executeQuery: (SQL)
    public static final int ERROR_DATABASE_NOT_SPECIFIED_OR_AVAILABLE = 0x230a; //Database not specified or available
    public static final int ERROR_INVALID_FOR_EXECUTE_UPDATE = 0x230b;  //not a valid sql for executeUpdate: (SQL)
    public static final int ERROR_INVALID_FOR_EXECUTE = 0x230c;         //not a valid sql for execute: (SQL)
    public static final int ERROR_PARAMETER_INDEX_OUT_RANGE = 0x230d;   // parameter index out of range
    public static final int ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED = 0x230e;    // connection already closed
    public static final int ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE = 0x230f;        //unknown sql type in tdengine
    public static final int ERROR_CANNOT_REGISTER_JNI_DRIVER = 0x2310;  // can't register JDBC-JNI driver
    public static final int ERROR_CANNOT_REGISTER_RESTFUL_DRIVER = 0x2311;  // can't register JDBC-RESTful driver
    public static final int ERROR_URL_NOT_SET = 0x2312;  // url is not set
    public static final int ERROR_INVALID_SQL = 0x2313;     // invalid sql
    public static final int ERROR_NUMERIC_VALUE_OUT_OF_RANGE = 0x2314;  // numeric value out of range
    public static final int ERROR_UNKNOWN_TAOS_TYPE_IN_TDENGINE = 0x2315; //unknown taos type in tdengine
    public static final int ERROR_UNKNOWN_TIMESTAMP_PERCISION = 0x2316;     // unknown timestamp precision

    public static final int ERROR_UNKNOWN = 0x2350;    //unknown error

    public static final int ERROR_SUBSCRIBE_FAILED = 0x2351;     // failed to create subscription
    public static final int ERROR_UNSUPPORTED_ENCODING = 0x2352; // Unsupported encoding
    public static final int ERROR_JNI_TDENGINE_ERROR = 0x2353;   // internal error of database
    public static final int ERROR_JNI_CONNECTION_NULL = 0x2354;  // JNI connection is NULL
    public static final int ERROR_JNI_RESULT_SET_NULL = 0x2355;  // invalid JNI result set
    public static final int ERROR_JNI_NUM_OF_FIELDS_0 = 0x2356;  // invalid num of fields
    public static final int ERROR_JNI_SQL_NULL = 0x2357;        // empty sql string
    public static final int ERROR_JNI_FETCH_END = 0x2358;       // fetch to the end of resultSet
    public static final int ERROR_JNI_OUT_OF_MEMORY = 0x2359;   // JNI alloc memory failed

    private static final Set<Integer> errorNumbers = new HashSet<>();

    static {
        errorNumbers.add(ERROR_CONNECTION_CLOSED);
        errorNumbers.add(ERROR_UNSUPPORTED_METHOD);
        errorNumbers.add(ERROR_INVALID_VARIABLE);
        errorNumbers.add(ERROR_STATEMENT_CLOSED);
        errorNumbers.add(ERROR_RESULTSET_CLOSED);
        errorNumbers.add(ERROR_INVALID_WITH_EXECUTEQUERY);
        errorNumbers.add(ERROR_INVALID_WITH_EXECUTEUPDATE);
        errorNumbers.add(ERROR_INVALID_FOR_EXECUTE_QUERY);
        errorNumbers.add(ERROR_DATABASE_NOT_SPECIFIED_OR_AVAILABLE);
        errorNumbers.add(ERROR_INVALID_FOR_EXECUTE_UPDATE);
        errorNumbers.add(ERROR_INVALID_FOR_EXECUTE);
        errorNumbers.add(ERROR_PARAMETER_INDEX_OUT_RANGE);
        errorNumbers.add(ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED);
        errorNumbers.add(ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE);
        errorNumbers.add(ERROR_CANNOT_REGISTER_JNI_DRIVER);
        errorNumbers.add(ERROR_CANNOT_REGISTER_RESTFUL_DRIVER);
        errorNumbers.add(ERROR_URL_NOT_SET);
        errorNumbers.add(ERROR_INVALID_SQL);
        errorNumbers.add(ERROR_NUMERIC_VALUE_OUT_OF_RANGE);
        errorNumbers.add(ERROR_UNKNOWN_TAOS_TYPE_IN_TDENGINE);
        errorNumbers.add(ERROR_UNKNOWN_TIMESTAMP_PERCISION);

        errorNumbers.add(ERROR_SUBSCRIBE_FAILED);
        errorNumbers.add(ERROR_UNSUPPORTED_ENCODING);
        errorNumbers.add(ERROR_JNI_TDENGINE_ERROR);
        errorNumbers.add(ERROR_JNI_CONNECTION_NULL);
        errorNumbers.add(ERROR_JNI_RESULT_SET_NULL);
        errorNumbers.add(ERROR_JNI_NUM_OF_FIELDS_0);
        errorNumbers.add(ERROR_JNI_SQL_NULL);
        errorNumbers.add(ERROR_JNI_FETCH_END);
        errorNumbers.add(ERROR_JNI_OUT_OF_MEMORY);
    }

    private TSDBErrorNumbers() {
    }

    public static boolean contains(int errorNumber) {
        return errorNumbers.contains(errorNumber);
    }
}
