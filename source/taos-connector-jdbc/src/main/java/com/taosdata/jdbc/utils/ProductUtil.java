package com.taosdata.jdbc.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProductUtil {
    private static final Logger log = LoggerFactory.getLogger(ProductUtil.class);
    private static final String PRODUCT_NAME;
    private static final String PRODUCT_VERSION;
    private static final String DRIVER_VERSION;
    private static final int DRIVER_MAJOR_VERSION;
    private static final int DRIVER_MINOR_VERSION;

    private static final String COMMIT_ID;
    private ProductUtil() {
    }
    private static InputStream loadProperties() {
        return ProductUtil.class.getClassLoader().getResourceAsStream("taos-jdbc-version.properties");
    }

    static {
        Properties props = System.getProperties();
        InputStream propertiesStream = loadProperties();
        if (propertiesStream != null) {
            try (InputStream stream = propertiesStream) {
                props.load(stream);
            } catch (IOException e) {
                log.error("load taos-jdbc-version.properties failed", e);
                //ignore
            }
        } else {
            log.error("Could not find taos-jdbc-version.properties on classpath");
        }
        PRODUCT_NAME = props.getProperty("PRODUCT_NAME");
        PRODUCT_VERSION = props.getProperty("PRODUCT_VERSION");
        DRIVER_VERSION = props.getProperty("DRIVER_VERSION");
        COMMIT_ID = props.getProperty("COMMIT_ID");

        int major = 3;
        int minor = 0;
        try {
            String[] versionParts = DRIVER_VERSION.split("\\.");
            major = Integer.parseInt(versionParts[0]);
            minor = Integer.parseInt(versionParts[1]);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            log.error("Failed to parse driver version: {}", DRIVER_VERSION, e);
        }
        DRIVER_MAJOR_VERSION = major;
        DRIVER_MINOR_VERSION = minor;
    }

    public static String getProductName() {
        return PRODUCT_NAME;
    }
    public static String getProductVersion() {
        return PRODUCT_VERSION;
    }
    public static String getDriverVersion() {
        return DRIVER_VERSION;
    }
    public static int getDriverMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }
    public static int getDriverMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }
    public static String getWsConnectorVersion() {
        return "jdbc-ws-v" + DRIVER_VERSION + "-" + COMMIT_ID;
    }
}
