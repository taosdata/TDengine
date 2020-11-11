package com.taosdata.demo.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class HikariCpBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        config.setJdbcUrl("jdbc:TAOS://" + host + ":6030");
        config.setUsername("root");
        config.setPassword("taosdata");

        config.setMaximumPoolSize(poolSize);
        config.setMinimumIdle(poolSize);
        HikariDataSource ds = new HikariDataSource(config);
        return ds;
    }
}
