package com.common;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReadProperties {

    public static String read(String key) {
        Properties properties = new Properties();
        InputStream inputStream = ReadProperties.class.getResourceAsStream("/runtime.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties.getProperty(key);
    }
}
