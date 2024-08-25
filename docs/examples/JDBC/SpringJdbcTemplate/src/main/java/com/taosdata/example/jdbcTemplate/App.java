package com.taosdata.example.jdbcTemplate;


import com.taosdata.example.jdbcTemplate.dao.ExecuteAsStatement;
import com.taosdata.example.jdbcTemplate.dao.WeatherDao;
import com.taosdata.example.jdbcTemplate.domain.Weather;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class App {

    private static Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) {

//        ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
//
//        ExecuteAsStatement executor = ctx.getBean(ExecuteAsStatement.class);
//        // drop database
//        executor.doExecute("drop database if exists test");
//        // create database
//        executor.doExecute("create database if not exists test");
//        //use database
//        executor.doExecute("use test");
//        // create table
//        executor.doExecute("create table if not exists test.weather (ts timestamp, temperature float, humidity int)");
//
//        WeatherDao weatherDao = ctx.getBean(WeatherDao.class);
//        Weather weather = new Weather(new Timestamp(new Date().getTime()), random.nextFloat() * 50.0f, random.nextInt(100));
//        // insert rows
//        int affectedRows = weatherDao.add(weather);
//        System.out.println("insert success " + affectedRows + " rows.");
//
//        // query for list
//        int limit = 10, offset = 0;
//        List<Weather> weatherList = weatherDao.queryForList(limit, offset);
//        for (Weather w : weatherList) {
//            System.out.println(w);
//        }
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        dataSource.setUrl("jdbc:TAOS-RS://vm98:6041/?batchfetch=true&user=root&password=taosdata");



        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        jdbcTemplate.query("select * from test.meters limit ?, ?",
                        (rs, rowNum) -> rs.getString("ts"), 1, 10)
                .forEach(System.out::println) ;

    }

}
