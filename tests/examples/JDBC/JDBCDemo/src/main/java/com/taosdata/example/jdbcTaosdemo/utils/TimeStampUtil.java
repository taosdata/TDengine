package com.taosdata.example.jdbcTaosdemo.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

public class TimeStampUtil {
    private static final String datetimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    public static long datetimeToLong(String dateTime) {
        SimpleDateFormat sdf = new SimpleDateFormat(datetimeFormat);
        try {
            return sdf.parse(dateTime).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static String longToDatetime(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat(datetimeFormat);
        return sdf.format(new Date(time));
    }

    public static void main(String[] args) throws ParseException {

//        Instant now = Instant.now();
//        System.out.println(now);
//        Instant years20Ago = now.minus(Duration.ofDays(365));
//        System.out.println(years20Ago);


    }


}
