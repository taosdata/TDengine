package com.taosdata.example.jdbcTemplate;


import com.taosdata.example.jdbcTemplate.dao.ExecuteAsStatement;
import com.taosdata.example.jdbcTemplate.dao.WeatherDao;
import com.taosdata.example.jdbcTemplate.domain.Weather;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:applicationContext.xml"})
public class BatcherInsertTest {


    @Autowired
    private WeatherDao weatherDao;
    @Autowired
    private ExecuteAsStatement executor;

    private static final int numOfRecordsPerTable = 1000;
    private static long ts = 1496732686000l;
    private static Random random = new Random(System.currentTimeMillis());

    @Before
    public void before() {
        // drop database
        executor.doExecute("drop database if exists test");
        // create database
        executor.doExecute("create database if not exists test");
        //use database
        executor.doExecute("use jdbctemplate_test");
        // create table
        executor.doExecute("create table if not exists weather (ts timestamp, temperature int, humidity float)");
    }

    @Test
    public void batchInsert() {
        List<Weather> weatherList = new ArrayList<>();
        for (int i = 0; i < numOfRecordsPerTable; i++) {
            ts += 1000;
            Weather weather = new Weather(new Timestamp(ts), random.nextFloat() * 50.0f, random.nextInt(100));
            weatherList.add(weather);
        }
        long start = System.currentTimeMillis();
        weatherDao.batchInsert(weatherList);
        long end = System.currentTimeMillis();
        System.out.println("batch insert(" + numOfRecordsPerTable + " rows) time cost ==========> " + (end - start) + " ms");

        int count = weatherDao.count();
        assertEquals(count, numOfRecordsPerTable);
    }

}
