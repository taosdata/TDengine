package com.taosdata.taosdemo.components;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Component
public class DataSourceFactory {

    private static DataSource instance;

    public static DataSource getInstance(String host, int port, String user, String password) throws IOException {
        if (instance == null) {
            synchronized (DataSourceFactory.class) {
                if (instance == null) {
                    InputStream is = DataSourceFactory.class.getClassLoader().getResourceAsStream("application.properties");
                    Properties properties = new Properties();
                    properties.load(is);

                    HikariConfig config = new HikariConfig();
                    if (properties.containsKey("jdbc.driver"))
                        config.setDriverClassName(properties.getProperty("jdbc.driver"));
                    else
                        config.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
                    if ("com.taosdata.jdbc.rs.RestfulDriver".equalsIgnoreCase(properties.getProperty("jdbc.driver")))
                        config.setJdbcUrl("jdbc:TAOS-RS://" + host + ":" + port + "/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC-8");
                    else
                        config.setJdbcUrl("jdbc:TAOS://" + host + ":" + port + "/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC-8");
                    config.setUsername(user);
                    config.setPassword(password);
                    // maximum-pool-size
                    if (properties.containsKey("hikari.maximum-pool-size"))
                        config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("hikari.maximum-pool-size")));
                    else
                        config.setMaximumPoolSize(500);
                    // minimum-idle
                    if (properties.containsKey("hikari.minimum-idle"))
                        config.setMinimumIdle(Integer.parseInt(properties.getProperty("hikari.minimum-idle")));
                    else
                        config.setMinimumIdle(100);
                    config.setMaxLifetime(0);
                    instance = new HikariDataSource(config);
                }
            }
        }
        return instance;
    }

}
