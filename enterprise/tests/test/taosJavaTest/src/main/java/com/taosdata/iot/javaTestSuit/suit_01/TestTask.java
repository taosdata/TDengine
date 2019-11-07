package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;

import java.sql.Connection;
import java.util.Properties;

public abstract class TestTask implements Runnable {

    public Connection getConnection(Properties properties) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.getConnection(properties);
        return connection;
    }

}
