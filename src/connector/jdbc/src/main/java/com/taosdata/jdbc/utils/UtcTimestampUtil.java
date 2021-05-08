package com.taosdata.jdbc.utils;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class UtcTimestampUtil {
    public static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-ddTHH:mm:ss.SSS+")
//            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

}
