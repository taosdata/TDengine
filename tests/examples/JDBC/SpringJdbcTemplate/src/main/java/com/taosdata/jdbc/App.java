package com.taosdata.jdbc;


import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

public class App {

    public static void main( String[] args ) {

        ApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");

        JdbcTemplate jdbcTemplate = (JdbcTemplate) ctx.getBean("jdbcTemplate");

        // create database
        jdbcTemplate.execute("create database if not exists db ");

        // create table
        jdbcTemplate.execute("create table if not exists db.tb (ts timestamp, temperature int, humidity float)");

        String insertSql = "insert into db.tb values(now, 23, 10.3) (now + 1s, 20, 9.3)";

        // insert rows
        int affectedRows = jdbcTemplate.update(insertSql);

        System.out.println("insert success " + affectedRows + " rows.");

        // query for list
        List<Map<String, Object>> resultList =  jdbcTemplate.queryForList("select * from db.tb");

        if(!CollectionUtils.isEmpty(resultList)){
            for (Map<String, Object> row : resultList){
                System.out.printf("%s, %d, %s\n", row.get("ts"), row.get("temperature"), row.get("humidity"));
            }
        }

    }

}
