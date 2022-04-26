package com.taosdata.example.jdbcTemplate;


import com.taosdata.example.jdbcTemplate.dao.ExecuteAsStatement;
import com.taosdata.example.jdbcTemplate.dao.WeatherDao;
import com.taosdata.example.jdbcTemplate.domain.Weather;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class App {

    private static Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) {

        ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");

        ExecuteAsStatement executor = ctx.getBean(ExecuteAsStatement.class);
        // drop database
        executor.doExecute("drop database if exists test");
        // create database
        executor.doExecute("create database if not exists test");
        //use database
        executor.doExecute("use test");
        // create table
        executor.doExecute("create table if not exists test.weather (ts timestamp, temperature int, humidity float)");

        WeatherDao weatherDao = ctx.getBean(WeatherDao.class);
        Weather weather = new Weather(new Timestamp(new Date().getTime()), random.nextFloat() * 50.0f, random.nextInt(100));
        // insert rows
        int affectedRows = weatherDao.add(weather);
        System.out.println("insert success " + affectedRows + " rows.");

        // query for list
        int limit = 10, offset = 0;
        List<Weather> weatherList = weatherDao.queryForList(limit, offset);
        for (Weather w : weatherList) {
            System.out.println(w);
        }

    }

}
