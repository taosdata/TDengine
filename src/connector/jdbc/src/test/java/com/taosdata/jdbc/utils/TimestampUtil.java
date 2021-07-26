package com.taosdata.jdbc.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampUtil {

    private static final String datetimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    public static long datetimeToLong(String dateTime) {
        SimpleDateFormat sdf = new SimpleDateFormat(datetimeFormat);
        try {
            return sdf.parse(dateTime).getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException("invalid datetime string >>> " + dateTime);
        }
    }

    public static String longToDatetime(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat(datetimeFormat);
        return sdf.format(new Date(time));
    }
}
