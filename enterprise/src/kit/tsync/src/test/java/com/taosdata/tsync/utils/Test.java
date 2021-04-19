package com.taosdata.tsync.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class Test {

    private static final long sessionTimeout = 30000;
    private static final long expirationInterval = 2000;

    public static void main(String[] args) throws ParseException {


//        long start = System.nanoTime();
//        do {
//            System.out.println(System.currentTimeMillis());
//        } while (System.nanoTime() - start < Duration.ofSeconds(5).toNanos());


//        System.out.println(new Timestamp(new Date().getTime()));
//        System.out.println(new Timestamp(new Time(12, 0, 0).getTime()));
//        for (int i = 0; i < 10; i++) {
//            long current = System.currentTimeMillis();
//            long expirationTime = calculateExpirationTime(current);
//            System.out.println("current: " + current + ", expirationTime: " + expirationTime);
//            TimeUnit.SECONDS.sleep(1);
//        }

//        String aaa = "Person{name='name_0', age=-1499755842, sex=true}";
//        aaa = aaa.replaceAll("'", "\\\\\\\\'");
//        System.out.println(aaa);
//        aaa = "'" + aaa + "'";
//        System.out.println(aaa);
//        String sql = "insert into p9 values(?,?,?)";
//        System.out.println(sql.replaceFirst("[?]", aaa));

//        DecimalFormat df = new DecimalFormat();
//        df.setMaximumFractionDigits(5);
//        System.out.println(df.format(Float.MAX_VALUE).replaceAll(",",""));

//        String s = new BigDecimal(new Float(Float.MAX_VALUE).toString()).divide(new BigDecimal(1), 5, BigDecimal.ROUND_HALF_UP).toString();
//        System.out.println(s);
    }

    private static long calculateExpirationTime(long current) {
        long expirationTime = current + sessionTimeout;
        expirationTime = (expirationTime / expirationInterval + 1) * expirationInterval;
        return expirationTime;
    }
}
