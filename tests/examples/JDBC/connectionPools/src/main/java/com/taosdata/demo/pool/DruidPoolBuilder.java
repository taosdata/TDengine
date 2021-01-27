package com.taosdata.demo.pool;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

public class DruidPoolBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        final String url = "jdbc:TAOS://" + host + ":6030";

        DruidDataSource dataSource = new DruidDataSource();
        // jdbc properties
        dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        dataSource.setUrl(url);
        dataSource.setUsername("root");
        dataSource.setPassword("taosdata");
        // pool configurations
        dataSource.setInitialSize(poolSize);
        dataSource.setMinIdle(poolSize);
        dataSource.setMaxActive(poolSize);
        dataSource.setMaxWait(30000);
        dataSource.setValidationQuery("select server_status()");
        return dataSource;
    }

}
