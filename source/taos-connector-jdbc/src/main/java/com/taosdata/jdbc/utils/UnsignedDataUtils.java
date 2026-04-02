package com.taosdata.jdbc.utils;

import java.math.BigInteger;

public class UnsignedDataUtils {

    private UnsignedDataUtils() {
    }

    public static short parseUTinyInt(byte val) {
        return (short) (val & 0xff);
    }

    public static int parseUSmallInt(short val) {
        return val & 0xffff;
    }

    public static long parseUInteger(int val) {
        return val & 0xffffffffL;
    }

    public static BigInteger parseUBigInt(long val) {

        if (val > 0) {
            return BigInteger.valueOf(val);
        }

        return new BigInteger(Long.toUnsignedString(val));
    }

}
