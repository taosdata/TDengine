package com.taosdata.example;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

public class DruidDemo {
// ANCHOR: connection_pool
public static void main(String[] args) throws Exception {
    String url = "jdbc:TAOS://127.0.0.1:6030/log";

    DruidDataSource dataSource = new DruidDataSource();
    // jdbc properties
    dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
    dataSource.setUrl(url);
    dataSource.setUsername("root");
    dataSource.setPassword("taosdata");
    // pool configurations
    dataSource.setInitialSize(10);
    dataSource.setMinIdle(10);
    dataSource.setMaxActive(10);
    dataSource.setMaxWait(30000);
    dataSource.setValidationQuery("SELECT SERVER_STATUS()");

    Connection connection = dataSource.getConnection(); // get connection
    Statement statement = connection.createStatement(); // get statement
    //query or insert
    // ...

    statement.close();
    connection.close(); // put back to connection pool
}
// ANCHOR_END: connection_pool
}
