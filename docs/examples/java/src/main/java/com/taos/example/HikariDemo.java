package com.taos.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.Statement;

public class HikariDemo {
    // ANCHOR: connection_pool
    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig();
        // jdbc properties
        config.setJdbcUrl("jdbc:TAOS-WS://127.0.0.1:6041/log");
        config.setUsername("root");
        config.setPassword("taosdata");
        // connection pool configurations
        config.setMinimumIdle(10); // minimum number of idle connection
        config.setMaximumPoolSize(10); // maximum number of connection in the pool
        config.setConnectionTimeout(30000); // maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0); // maximum life time for each connection
        config.setIdleTimeout(0); // max idle time for recycle idle connection
        config.setConnectionTestQuery("SELECT 1"); // validation query

        HikariDataSource dataSource = new HikariDataSource(config); // create datasource

        Connection connection = dataSource.getConnection(); // get connection
        Statement statement = connection.createStatement(); // get statement

        // query or insert
        // ...
        statement.close();
        connection.close(); // put back to connection pool
        dataSource.close();
    }
    // ANCHOR_END: connection_pool
}
