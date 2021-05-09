package com.taosdata.jdbc.utils;

public class NullType {
    private static final byte NULL_BOOL_VAL = 0x2;
    private static final String NULL_STR = "null";

    public String toString() {
        return NullType.NULL_STR;
    }

    public static boolean isBooleanNull(byte val) {
        return val == NullType.NULL_BOOL_VAL;
    }

    public static boolean isTinyIntNull(byte val) {
        return val == Byte.MIN_VALUE;
    }

    public static boolean isSmallIntNull(short val) {
        return val == Short.MIN_VALUE;
    }

    public static boolean isIntNull(int val) {
        return val == Integer.MIN_VALUE;
    }

    public static boolean isBigIntNull(long val) {
        return val == Long.MIN_VALUE;
    }

    public static boolean isFloatNull(float val) {
        return Float.isNaN(val);
    }

    public static boolean isDoubleNull(double val) {
        return Double.isNaN(val);
    }

    public static boolean isBinaryNull(byte[] val, int length) {
        if (length != Byte.BYTES) {
            return false;
        }

        return val[0] == 0xFF;
    }

    public static boolean isNcharNull(byte[] val, int length) {
        if (length != Integer.BYTES) {
            return false;
        }

        return (val[0] & val[1] & val[2] & val[3]) == 0xFF;
    }

}
