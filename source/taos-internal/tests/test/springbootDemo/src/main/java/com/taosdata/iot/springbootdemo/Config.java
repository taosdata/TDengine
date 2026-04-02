package com.taosdata.iot.springbootdemo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @author Jiangyi Hou
 * @since 18-12-13
 */

@Configuration
@EnableAutoConfiguration
@ComponentScan
public class Config {

    @Bean(name="hikariDatasource")
    @Qualifier("hikariDatasource")
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    public DataSource hikariDatasource() {
        System.out.println("Configuring datasource...");
        HikariConfig config = new HikariConfig("/home/jyhou/workspace/taosdata/test/springbootDemo/src/hikari.properties");
        HikariDataSource dataSource = new HikariDataSource(config);
//        DataSource dataSource = DataSourceBuilder.create().type(com.zaxxer.hikari.HikariDataSource.class).build();
//        DataSource dataSource = DataSourceBuilder.create().build();
        System.out.println("Data source: " + dataSource.getClass().getName());
        return dataSource;
    }
}
