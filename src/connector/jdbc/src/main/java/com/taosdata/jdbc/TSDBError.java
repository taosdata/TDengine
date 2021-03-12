package com.taosdata.jdbc;

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;

public class TSDBError {
    private static Map<Integer, String> TSDBErrorMap = new HashMap<>();

    static {
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, "connection already closed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "this operation is NOT supported currently!");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid variables");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, "statement is closed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED, "resultSet is closed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_BATCH_IS_EMPTY, "Batch is empty!");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEQUERY, "Can not issue data manipulation statements with executeQuery()");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEUPDATE, "Can not issue SELECT via executeUpdate()");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_QUERY, "invalid sql for executeQuery: (?)");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_DATABASE_NOT_SPECIFIED_OR_AVAILABLE, "Database not specified or available");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_UPDATE, "invalid sql for executeUpdate: (?)");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE, "invalid sql for execute: (?)");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "parameter index out of range");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED, "connection already closed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE, "unknown sql type in tdengine");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_CANNOT_REGISTER_JNI_DRIVER, "can't register JDBC-JNI driver");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_CANNOT_REGISTER_RESTFUL_DRIVER, "can't register JDBC-RESTful driver");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_URL_NOT_SET, "url is not set");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_SQL, "invalid sql");

        /**************************************************/
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNKNOWN, "unknown error");
        /**************************************************/
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_SUBSCRIBE_FAILED, "failed to create subscription");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNSUPPORTED_ENCODING, "Unsupported encoding");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_TDENGINE_ERROR, "internal error of database");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL, "JNI connection is NULL");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL, "JNI result set is NULL");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0, "invalid num of fields");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_SQL_NULL, "empty sql string");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_FETCH_END, "fetch to the end of resultSet");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY, "JNI alloc memory failed");
    }

    public static SQLException createSQLException(int errorCode) {
        String message;
        if (TSDBErrorNumbers.contains(errorCode))
            message = TSDBErrorMap.get(errorCode);
        else
            message = TSDBErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);
        return createSQLException(errorCode, message);
    }

    public static SQLException createSQLException(int errorCode, String message) {
        // throw SQLFeatureNotSupportedException
        if (errorCode == TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD)
            return new SQLFeatureNotSupportedException(message, "", errorCode);
        // throw SQLClientInfoException
        if (errorCode == TSDBErrorNumbers.ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED)
            return new SQLClientInfoException(message, null);

        if (errorCode > 0x2300 && errorCode < 0x2350)
            // JDBC exception's error number is less than 0x2350
            return new SQLException("ERROR (" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
        if (errorCode > 0x2350 && errorCode < 0x2400)
            // JNI exception's error number is large than 0x2350
            return new SQLException("JNI ERROR (" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
        return new SQLException("TDengine ERROR (" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
    }

    public static RuntimeException createRuntimeException(int errorCode, Throwable t) {
        String message = TSDBErrorMap.get(errorCode);
        return new RuntimeException("ERROR (" + Integer.toHexString(errorCode) + "): " + message, t);
    }

    public static SQLWarning createSQLWarning(String message) {
        return new SQLWarning(message);
    }
}