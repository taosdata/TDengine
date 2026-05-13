package com.taosdata.jdbc.utils;

public class BlockUtil {
    private BlockUtil() {
    }
    private static final int[] MASKS = {128, 64, 32, 16, 8, 4, 2, 1};

    public static boolean isNull(byte[] c, int n) {
        int position = n >>> 3;
        int index = n & 0x7;
        return (c[position] & MASKS[index]) != 0;
    }
}
