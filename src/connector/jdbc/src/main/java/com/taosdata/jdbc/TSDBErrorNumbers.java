package com.taosdata.jdbc;

public class TSDBErrorNumbers {

    public static final int ERROR_CONNECTION_CLOSED = 0x2301;   // connection already closed
    public static final int ERROR_UNSUPPORTED_METHOD = 0x2302;  //this operation is NOT supported currently!
    public static final int ERROR_INVALID_VARIABLE = 0x2303;    //invalid variables
    public static final int ERROR_STATEMENT_CLOSED = 0x2304;    //statement already closed
    public static final int ERROR_RESULTSET_CLOSED = 0x2305;    //resultSet is closed

    public static final int ERROR_SUBSCRIBE_FAILED = 0x2350;    //failed to create subscription

    private TSDBErrorNumbers() {
    }
}
