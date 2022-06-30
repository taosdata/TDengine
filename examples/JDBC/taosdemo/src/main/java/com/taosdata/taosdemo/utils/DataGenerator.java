package com.taosdata.taosdemo.utils;

import java.util.Random;

public class DataGenerator {
    private static Random random = new Random(System.currentTimeMillis());
    private static final String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    // "timestamp", "int", "bigint", "float", "double", "binary(64)", "smallint", "tinyint", "bool", "nchar(64)",

    public static Object randomValue(String type) {
        int length = 64;
        if (type.contains("(")) {
            length = Integer.parseInt(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
            type = type.substring(0, type.indexOf("("));
        }
        switch (type.trim().toLowerCase()) {
            case "timestamp":
                return randomTimestamp();
            case "int":
                return randomInt();
            case "bigint":
                return randomBigint();
            case "float":
                return randomFloat();
            case "double":
                return randomDouble();
            case "binary":
                return randomBinary(length);
            case "smallint":
                return randomSmallint();
            case "tinyint":
                return randomTinyint();
            case "bool":
                return randomBoolean();
            case "nchar":
                return randomNchar(length);
            default:
                throw new IllegalArgumentException("Unexpected value: " + type);
        }
    }

    public static Long randomTimestamp() {
        long start = System.currentTimeMillis();
        return randomTimestamp(start, start + 60l * 60l * 1000l);
    }

    public static Long randomTimestamp(Long start, Long end) {
        return start + (long) random.nextInt((int) (end - start));
    }

    public static String randomNchar(int length) {
        return randomChinese(length);
    }

    public static Boolean randomBoolean() {
        return random.nextBoolean();
    }

    public static Integer randomTinyint() {
        return randomInt(-127, 127);
    }

    public static Integer randomSmallint() {
        return randomInt(-32767, 32767);
    }

    public static String randomBinary(int length) {
        return randomString(length);
    }

    public static String randomString(int length) {
        String zh_en = "";
        for (int i = 0; i < length; i++) {
            zh_en += alphabet.charAt(random.nextInt(alphabet.length()));
        }
        return zh_en;
    }

    public static String randomChinese(int length) {
        String zh_cn = "";
        int bottom = Integer.parseInt("4e00", 16);
        int top = Integer.parseInt("9fa5", 16);

        for (int i = 0; i < length; i++) {
            char c = (char) (random.nextInt(top - bottom + 1) + bottom);
            zh_cn += new String(new char[]{c});
        }
        return zh_cn;
    }

    public static Double randomDouble() {
        return randomDouble(0, 100);
    }

    public static Double randomDouble(double bottom, double top) {
        return bottom + (top - bottom) * random.nextDouble();
    }

    public static Float randomFloat() {
        return randomFloat(0, 100);
    }

    public static Float randomFloat(float bottom, float top) {
        return bottom + (top - bottom) * random.nextFloat();
    }

    public static Long randomBigint() {
        return random.nextLong();
    }

    public static Integer randomInt(int bottom, int top) {
        return bottom + random.nextInt((top - bottom));
    }

    public static Integer randomInt() {
        return randomInt(0, 100);
    }

}
