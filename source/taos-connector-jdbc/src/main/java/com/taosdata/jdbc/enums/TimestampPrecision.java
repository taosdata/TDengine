package com.taosdata.jdbc.enums;

public class TimestampPrecision {
    public static final int MS = 0;
    public static final int US = 1;
    public static final int NS = 2;
    public static final int UNKNOWN = 9999;

    public static int getPrecision(String precision) {
        if (precision == null)
            return UNKNOWN;
        String trim = precision.trim();
        switch (trim) {
            case "ms":
                return MS;
            case "us":
                return US;
            case "ns":
                return NS;
            default:
                return UNKNOWN;
        }
    }
}
