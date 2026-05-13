package com.taosdata.jdbc.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampUtil {

    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public static long datetimeToLong(String dateTime) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
        try {
            return sdf.parse(dateTime).getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException("invalid datetime string >>> " + dateTime);
        }
    }

    public static String longToDatetime(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
        return sdf.format(new Date(time));
    }
}
