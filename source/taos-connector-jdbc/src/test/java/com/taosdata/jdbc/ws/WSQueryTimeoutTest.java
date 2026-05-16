package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket master slave test", author = "yjshe", version = "3.2.11")
@FixMethodOrder
public class WSQueryTimeoutTest {
        @Description("query")
    @Test
    public void queryTimeoutTest() throws Exception  {
        TaosAdapterMock mockA = new TaosAdapterMock();

        mockA.start();

        Properties properties = new Properties();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + mockA.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        boolean haveRecords = false;
        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement1 = connection.createStatement();
                Statement statement2 = connection.createStatement()) {
            statement1.setQueryTimeout(1);
            ResultSet resultSet1 = statement1.executeQuery("show databases;");
            while (resultSet1.next()) {
                String dbName = resultSet1.getString(1);
                haveRecords = true;
            }

            Assert.assertTrue(haveRecords);
            resultSet1.close();
            mockA.setDelayMillis(2000);

            haveRecords = false;
            ResultSet resultSet2 = statement2.executeQuery("show databases;");
            while (resultSet2.next()) {
                String dbName = resultSet2.getString(1);
                haveRecords = true;
            }
            Assert.assertTrue(haveRecords);
            resultSet2.close();
        }
        mockA.stop();
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }
}