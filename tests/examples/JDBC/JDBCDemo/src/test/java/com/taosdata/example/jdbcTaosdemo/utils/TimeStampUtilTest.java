package com.taosdata.example.jdbcTaosdemo.utils;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.junit.Assert.*;

public class TimeStampUtilTest {

    @Test
    public void datetimeToLong() {
        final String startTime = "2005-01-01 00:00:00.000";
        long start = TimeStampUtil.datetimeToLong(startTime);
        assertEquals(1104508800000l, start);
    }

    @Test
    public void longToDatetime() {
        String datetime = TimeStampUtil.longToDatetime(1510000000000L);
        assertEquals("2017-11-07 04:26:40.000", datetime);
    }

    @Test
    public void getStartDateTime() {
        int keep = 365;

        Instant end = Instant.now();
        System.out.println(end.toString());
        System.out.println(end.toEpochMilli());

        Instant start = end.minus(Duration.ofDays(keep));
        System.out.println(start.toString());
        System.out.println(start.toEpochMilli());

        int numberOfRecordsPerTable = 10;
        long timeGap = ChronoUnit.MILLIS.between(start, end) / (numberOfRecordsPerTable - 1);
        System.out.println(timeGap);

        System.out.println("===========================");
        for (int i = 0; i < numberOfRecordsPerTable; i++) {
            long ts = start.toEpochMilli() + (i * timeGap);
            System.out.println(i + " : " + ts);
        }
    }
}