package com.taosdata.jdbc.utils;

import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalUtil {

    public static BigDecimal getBigDecimal(byte[] original, int scale) {
        // reverse original array
        int left = 0;
        int right = original.length - 1;
        while (left < right) {
            byte temp = original[left];
            original[left] = original[right];
            original[right] = temp;
            left++;
            right--;
        }

        BigInteger value = new BigInteger(original);
        return new BigDecimal(value).movePointLeft(scale);
    }
}
