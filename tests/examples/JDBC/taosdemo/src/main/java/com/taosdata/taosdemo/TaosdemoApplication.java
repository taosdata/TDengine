package com.taosdata.taosdemo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan(basePackages = {"com.taosdata.taosdemo.mapper"})
@SpringBootApplication
public class TaosdemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(TaosdemoApplication.class, args);
    }

}
