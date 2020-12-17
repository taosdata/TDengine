package com.taosdata.taosdemo;

import com.zaxxer.hikari.HikariDataSource;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import javax.sql.DataSource;

@MapperScan(basePackages = {"com.taosdata.taosdemo.mapper"})
@SpringBootApplication
public class TaosdemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(TaosdemoApplication.class, args);

    }

}
