package com.taosdata.taosdemo.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeStampUtil {

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

    public static class TimeTuple {
        public Long start;
        public Long end;
        public Long timeGap;

        TimeTuple(long start, long end, long timeGap) {
            this.start = start;
            this.end = end;
            this.timeGap = timeGap;
        }
    }

    public static TimeTuple range(long start, long timeGap, long size) {
        long now = System.currentTimeMillis();
        if (timeGap < 1)
            timeGap = 1;
        if (start == 0)
            start = now - size * timeGap;

        // If size is less than 1, throw an exception
        if (size < 1)
            throw new IllegalArgumentException("size less than 1.");
        // If timeGap is 1 and it exceeds the limit, move start forward
        if (start + size > now) {
            start = now - size;
            return new TimeTuple(start, now, 1);
        }
        long end = start + (long) (timeGap * size);
        if (end > now) {
            // Compress timeGap
            end = now;
            double gap = (end - start) / (size * 1.0f);
            if (gap < 1.0f) {
                timeGap = 1;
                start = end - size;
            } else {
                timeGap = (long) gap;
                end = start + (long) (timeGap * size);
            }
        }
        return new TimeTuple(start, end, timeGap);
    }
}
