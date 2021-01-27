package com.taosdata.demo.pool;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.util.Properties;

public class DruidPoolBuilder {

    public static DataSource getDataSource(String host, int poolSize) throws Exception {
        final String url = "jdbc:TAOS://" + host + ":6030";

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
        dataSource.setValidationQuery("select server_status()");
        return dataSource;
    }

}
