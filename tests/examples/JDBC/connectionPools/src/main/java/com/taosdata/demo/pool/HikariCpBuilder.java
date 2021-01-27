package com.taosdata.demo.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class HikariCpBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        HikariConfig config = new HikariConfig();
        // jdbc properties
        config.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        config.setJdbcUrl("jdbc:TAOS://" + host + ":6030");
        config.setUsername("root");
        config.setPassword("taosdata");
        // pool configurations
        config.setMinimumIdle(3);           //minimum number of idle connection
        config.setMaximumPoolSize(10);      //maximum number of connection in the pool
        config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
        config.setIdleTimeout(0);       // max idle time for recycle idle connection
        config.setConnectionTestQuery("select server_status()"); //validation query

        HikariDataSource ds = new HikariDataSource(config);
        return ds;
    }
}
