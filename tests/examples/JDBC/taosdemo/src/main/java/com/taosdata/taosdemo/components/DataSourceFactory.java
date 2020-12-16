package com.taosdata.taosdemo.components;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class DataSourceFactory {

    private static DataSource instance;

    public static DataSource getInstance(String host, int port, String user, String password) {
        if (instance == null) {
            synchronized (DataSourceFactory.class) {
                if (instance == null) {
                    HikariConfig config = new HikariConfig();
                    config.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
                    config.setJdbcUrl("jdbc:TAOS://" + host + ":" + port + "/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC-8");
                    config.setUsername(user);
                    config.setPassword(password);
                    config.setMaxLifetime(0);
                    config.setMaximumPoolSize(500);
                    config.setMinimumIdle(100);
                    instance = new HikariDataSource(config);
                }
            }
        }
        return instance;
    }

}
