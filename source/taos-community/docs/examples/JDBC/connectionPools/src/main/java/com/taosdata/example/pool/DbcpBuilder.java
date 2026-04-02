package com.taosdata.example.pool;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;

public class DbcpBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        ds.setUrl("jdbc:TAOS://" + host + ":6030");
        ds.setUsername("root");
        ds.setPassword("taosdata");

        ds.setMaxTotal(poolSize);
        ds.setMinIdle(poolSize);
        ds.setInitialSize(poolSize);
        return ds;
    }
}
