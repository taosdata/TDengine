package com.taosdata.jdbc;

import java.sql.SQLException;
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
        /**************************************************/
        TSDBErrorMap.put(TSDBErrorNumbers.ERROR_SUBSCRIBE_FAILED, "failed to create subscription");
    }

    public static String wrapErrMsg(String msg) {
        return "TDengine Error: " + msg;
    }

    public static SQLException createSQLException(int errorNumber) {
        // JDBC exception code is less than 0x2350
        if (errorNumber <= 0x2350)
            return new SQLException(TSDBErrorMap.get(errorNumber));
        // JNI exception code is
        return new SQLException(wrapErrMsg(TSDBErrorMap.get(errorNumber)));
    }
}
