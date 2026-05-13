package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class AbstractDatabaseMetaDataVesionTest {
    Connection connection;
    static final String host = TestEnvUtil.getHost();

    @Test
    public void testJni() throws IOException, SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url);
        DatabaseMetaData metaData = connection.getMetaData();
        Properties properties = new Properties();
        properties.load(AbstractDatabaseMetaDataVesionTest.class.getClassLoader().getResourceAsStream("taos-jdbc-version.properties"));
        String productName = properties.getProperty("PRODUCT_NAME");
        String driverVersion = properties.getProperty("DRIVER_VERSION");
        Assert.assertNotNull(metaData.getDatabaseProductVersion());
        Assert.assertEquals(productName, metaData.getDatabaseProductName());
        Assert.assertEquals(driverVersion, metaData.getDriverVersion());
        Assert.assertNotEquals(0, metaData.getDriverMajorVersion());
        Assert.assertNotEquals(0, metaData.getDriverMinorVersion());
        connection.close();
    }

    @Test
    public void testRest() throws IOException, SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url);
        DatabaseMetaData metaData = connection.getMetaData();
        Properties properties = new Properties();
        properties.load(AbstractDatabaseMetaDataVesionTest.class.getClassLoader().getResourceAsStream("taos-jdbc-version.properties"));
        String productName = properties.getProperty("PRODUCT_NAME");
        String driverVersion = properties.getProperty("DRIVER_VERSION");
        Assert.assertNotNull(metaData.getDatabaseProductVersion());
        Assert.assertEquals(productName, metaData.getDatabaseProductName());
        Assert.assertEquals(driverVersion, metaData.getDriverVersion());
        Assert.assertNotEquals(0, metaData.getDriverMajorVersion());
        Assert.assertNotEquals(0, metaData.getDriverMinorVersion());
        connection.close();
    }

    @Test
    public void testWebsocket() throws IOException, SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties config = new Properties();
        config.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, config);
        DatabaseMetaData metaData = connection.getMetaData();
        Properties properties = new Properties();
        properties.load(AbstractDatabaseMetaDataVesionTest.class.getClassLoader().getResourceAsStream("taos-jdbc-version.properties"));
        String productName = properties.getProperty("PRODUCT_NAME");
        String driverVersion = properties.getProperty("DRIVER_VERSION");
        Assert.assertNotNull(metaData.getDatabaseProductVersion());
        Assert.assertEquals(productName, metaData.getDatabaseProductName());
        Assert.assertEquals(driverVersion, metaData.getDriverVersion());
        Assert.assertNotEquals(0, metaData.getDriverMajorVersion());
        Assert.assertNotEquals(0, metaData.getDriverMinorVersion());
        connection.close();
    }

}