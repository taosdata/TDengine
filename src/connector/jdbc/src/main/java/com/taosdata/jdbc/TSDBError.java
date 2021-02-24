package com.taosdata.jdbc;

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_QUERY, "not a valid sql for executeQuery: (?)");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_DATABASE_NOT_SPECIFIED_OR_AVAILABLE, "Database not specified or available");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_UPDATE, "not a valid sql for executeUpdate: (?)");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE, "not a valid sql for execute: (?)");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, "parameter index out of range");

        /**************************************************/
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNKNOWN, "unknown error");
        /**************************************************/
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_SUBSCRIBE_FAILED, "failed to create subscription");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_UNSUPPORTED_ENCODING, "Unsupported encoding");

        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_TDENGINE_ERROR, "internal error of database!");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL, "JNI connection already closed!");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL, "invalid JNI result set!");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0, "invalid num of fields!");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_SQL_NULL, "empty sql string!");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_FETCH_END, "fetch to the end of resultset");
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_JNI_OUT_OF_MEMORY, "JNI alloc memory failed!");
    }

    public static String wrapErrMsg(String msg) {
        return "TDengine Error: " + msg;
    }

    public static SQLException createSQLException(int errorNumber) {
        return createSQLException(errorNumber, null);
    }

    public static SQLException createSQLException(int errorNumber, String message) {
        if (message == null || message.isEmpty()) {
            if (TSDBErrorNumbers.contains(errorNumber))
                message = TSDBErrorMap.get(errorNumber);
            else
                message = TSDBErrorMap.get(TSDBErrorNumbers.ERROR_UNKNOWN);
        }

        if (errorNumber == TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD)
            return new SQLFeatureNotSupportedException(message);

        if (errorNumber < TSDBErrorNumbers.ERROR_UNKNOWN)
            // JDBC exception's error number is less than 0x2350
            return new SQLException("ERROR (" + Integer.toHexString(errorNumber) + "): " + message);
        // JNI exception's error number is large than 0x2350
        return new SQLException("TDengine ERROR (" + Integer.toHexString(errorNumber) + "): " + message);
    }

    public static SQLClientInfoException createSQLClientInfoException(int errorNumber) {
        return new SQLClientInfoException();
    }
}
