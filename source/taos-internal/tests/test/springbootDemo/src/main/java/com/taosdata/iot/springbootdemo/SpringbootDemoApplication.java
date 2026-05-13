package com.taosdata.iot.springbootdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringbootDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootDemoApplication.class, args);
//        HikariConfig config = new HikariConfig("/home/jyhou/workspace/taosdata/test/springbootDemo/src/hikari.properties");
//        System.out.println(config.getDriverClassName());
//        System.out.println(config.getJdbcUrl());
//        System.out.println(config.getConnectionTimeout());
//        System.out.println(config.getMaximumPoolSize());
//        System.out.println(config.getMinimumIdle());
//        System.out.println(config.getUsername());
//        System.out.println(config.getPassword());
//        HikariDataSource ds = new HikariDataSource(config);
    }

}
