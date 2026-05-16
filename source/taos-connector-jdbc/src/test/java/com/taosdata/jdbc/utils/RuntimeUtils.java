package com.taosdata.jdbc.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 */
public class RuntimeUtils {

    /**
     * another method to get TDengine version,Local method run 'taos -V'
     * to help run getDatabaseProductVersion test case
     * @return
     */
    public static String getLocalTDengineVersion() {
        Process proc = null;
        try {
            proc = Runtime.getRuntime().exec("taos -V");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

        try (InputStream is =  proc.getInputStream();
             InputStreamReader isr = new InputStreamReader(is,"UTF-8");
             BufferedReader br = new BufferedReader(isr)) {

            // just first line
            String output = br.readLine();

            if (StringUtils.isEmpty(output)) {
                return "";
            }

            String[] array = output.split("\\s+");
            if (array == null || array.length != 2) {
                return "";
            }

            return array[1];
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
