package com.taosdata.demo.pool;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

public class DruidPoolBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        final String url = "jdbc:TAOS://" + host + ":6030";

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        dataSource.setUsername("root");
        dataSource.setPassword("taosdata");

        //初始连接数，默认0
        dataSource.setInitialSize(poolSize);
        //最大连接数，默认8
        dataSource.setMaxActive(poolSize);
        //最小闲置数
        dataSource.setMinIdle(poolSize);
        //获取连接的最大等待时间，单位毫秒
        dataSource.setMaxWait(2000);

        return dataSource;
    }


}
