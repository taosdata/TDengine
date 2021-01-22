package com.taosdata.demo.pool;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

public class DruidPoolBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        final String url = "jdbc:TAOS://" + host + ":6030";

        DruidDataSource dataSource = new DruidDataSource();
        // jdbc properties
        dataSource.setUrl(url);
        dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        dataSource.setUsername("root");
        dataSource.setPassword("taosdata");

        // pool configurations
        dataSource.setInitialSize(poolSize);//初始连接数，默认0
        dataSource.setMinIdle(poolSize);//最小闲置数
        dataSource.setMaxActive(poolSize);//最大连接数，默认8
        dataSource.setMaxWait(30000);//获取连接的最大等待时间，单位毫秒
        dataSource.setValidationQuery("select server_status()");
        return dataSource;
    }


}
