package com.taosdata.example.jdbcTaosdemo.utils;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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

    public static void main(String[] args) {
        final String startTime = "2005-01-01 00:00:00.000";

        long start = TimeStampUtil.datetimeToLong(startTime);
        System.out.println(start);

        String datetime = TimeStampUtil.longToDatetime(1519833600000L);
        System.out.println(datetime);
    }


}
