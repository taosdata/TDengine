package com.taosdata.demo.pool;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;

public class C3p0Builder {

    public static DataSource getDataSource(String host, int poolSize) {
        ComboPooledDataSource ds = new ComboPooledDataSource();

        try {
            ds.setDriverClass("com.taosdata.jdbc.TSDBDriver");
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        ds.setJdbcUrl("jdbc:TAOS://" + host + ":6030");
        ds.setUser("root");
        ds.setPassword("taosdata");

        ds.setMinPoolSize(poolSize);
        ds.setMaxPoolSize(poolSize);
        ds.setAcquireIncrement(5);
        return ds;
    }
}
