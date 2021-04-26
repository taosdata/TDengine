package com.taosdata.jdbc.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private static Pattern ptn = Pattern.compile(".*?'");

    public static String escapeSingleQuota(String origin) {
        Matcher m = ptn.matcher(origin);
        StringBuffer sb = new StringBuffer();
        int end = 0;
        while(m.find()) {
            end = m.end();
            String seg = origin.substring(m.start(), end);
            int len = seg.length();
            if (len == 1) {
                if ('\'' == seg.charAt(0)) {
                    sb.append("\\'");
                } else {
                    sb.append(seg);
                }
            } else { // len > 1
                sb.append(seg.substring(0, seg.length() - 2));
                char lastcSec = seg.charAt(seg.length() - 2);
                if (lastcSec == '\\') {
                    sb.append("\\'");
                } else {
                    sb.append(lastcSec);
                    sb.append("\\'");
                }
            }
        }

        if (end < origin.length()) {
            sb.append(origin.substring(end));
        }
        return sb.toString();
    }
}
