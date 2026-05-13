package com.taosdata.jdbc;

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class TSDBError {
    private static final Map<Integer, String> TSDBErrorMap = new HashMap<>();
    private TSDBError(){}

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
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE, "numeric value out of range");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNKNOWN_TAOS_TYPE, "unknown taos type in tdengine");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNKNOWN_TIMESTAMP_PRECISION, "unknown timestamp precision");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED, "user is required");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED, "password is required");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_JSON_FORMAT, "invalid json format");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT, "create connection with server timeout");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, "query timeout");

        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION, "restful client query exception");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TYPE_CONVERT_EXCEPTION, "type convert exception");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_VERSION_INCOPMATIABLE, "TDengine version incompatible");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_RESOURCE_FREEED, "resource has been freed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_BLOB_UNSUPPORTED_IN_SERVER, "BLOB is unsupported on the server");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_LINE_BIND_MODE_UNSUPPORTED_IN_SERVER, "line bind mode is unsupported on the server");

        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNKNOWN, "unknown error");

        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_SUBSCRIBE_FAILED, "failed to create subscription");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNSUPPORTED_ENCODING, "Unsupported encoding");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_TDENGINE_ERROR, "internal error of database, please see taoslog for more details");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL, "JNI connection is NULL");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL, "JNI result set is NULL");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0, "invalid num of fields");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_SQL_NULL, "empty sql string");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_FETCH_END, "fetch to the end of resultSet");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY, "JNI alloc memory failed, please see taoslog for more details");

        // consumer
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_CONF_NULL, "consumer config reference has been destroyed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_CONF_KEY_NULL, "configs contain empty key, failed to set consumer property");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_CONF_VALUE_NULL, "consumer configs contain empty value, failed to set consumer property");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_CONF_ERROR, "consumer config error");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NULL, "topic reference has been destroyed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_TOPIC_NAME_NULL, "failed to set consumer topic, topic name is empty");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_CONSUMER_NULL, "consumer reference is null or has been destroyed");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_CONSUMER_CREATE_ERROR, "consumer create error");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_SEEK_OFFSET, "seek offset must not be a negative number");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_TMQ_VGROUP_NOT_FOUND, "vGroup not found in result set");

        // websocket
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_FW_WRITE_ERROR, "fast writer write error");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_WS_MSG_NEED_RESEND, "the msg need resend after reconnect");
    }

    public static SQLException createSQLException(int errorCode) {
        String message;
        if (TSDBErrorMap.containsKey(errorCode))
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
            return new SQLException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
        if (errorCode > 0x2350 && errorCode < 0x2370)
            // JNI exception's error number is large than 0x2350
            return new SQLException("JNI ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);

        if (errorCode > 0x2370 && errorCode < 0x2400)
            return new SQLException("Consumer ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);

        return new SQLException("TDengine ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, "", errorCode);
    }

    public static RuntimeException createRuntimeException(int errorCode, Throwable t) {
        String message = TSDBErrorMap.get(errorCode);
        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message, t);
    }

    public static SQLWarning createSQLWarning(String message) {
        return new SQLWarning(message);
    }


    // paramter size is greater than 1
    public static SQLException undeterminedExecutionError() {
        return new SQLException("Please either call clearBatch() to clean up context first, or use executeBatch() instead", (String) null);
    }

    public static IllegalArgumentException createIllegalArgumentException(int errorCode) {
        String message;
        if (TSDBErrorMap.containsKey(errorCode))
            message = TSDBErrorMap.get(errorCode);
        else
            message = TSDBErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new IllegalArgumentException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static RuntimeException createRuntimeException(int errorCode) {
        String message;
        if (TSDBErrorMap.containsKey(errorCode))
            message = TSDBErrorMap.get(errorCode);
        else
            message = TSDBErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static RuntimeException createRuntimeException(int errorCode, String message) {
        return new RuntimeException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static IllegalStateException createIllegalStateException(int errorCode) {
        String message;
        if (TSDBErrorMap.containsKey(errorCode))
            message = TSDBErrorMap.get(errorCode);
        else
            message = TSDBErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);

        return new IllegalStateException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static TimeoutException createTimeoutException(int errorCode, String message) {
        return new TimeoutException("ERROR (0x" + Integer.toHexString(errorCode) + "): " + message);
    }

    public static String getErrorMessage(int errorCode) {
        if (TSDBErrorMap.containsKey(errorCode))
            return TSDBErrorMap.get(errorCode);
        else
            return TSDBErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);
    }
}