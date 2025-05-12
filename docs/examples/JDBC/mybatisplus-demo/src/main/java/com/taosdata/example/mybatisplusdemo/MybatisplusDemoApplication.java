package com.taosdata.example.mybatisplusdemo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.taosdata.example.mybatisplusdemo.mapper")
public class MybatisplusDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(MybatisplusDemoApplication.class, args);
    }

}