package com.taosdata.taosdemo.dao.impl;

import com.taosdata.taosdemo.dao.DatabaseMapper;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

public class DatabaseMapperImpl implements DatabaseMapper {

    private JdbcTemplate jdbcTemplate = new JdbcTemplate();

    @Override
    public void createDatabase(String dbname) {
        jdbcTemplate.execute("create database if not exists " + dbname);
    }

    @Override
    public void dropDatabase(String dbname) {
        jdbcTemplate.update("drop database if exists" + dbname);
    }

    @Override
    public void createDatabaseWithParameters(Map<String, String> map) {
        jdbcTemplate.execute(SqlSpeller.createDatabase(map));
    }

    @Override
    public void useDatabase(String dbname) {
        jdbcTemplate.execute("use " + dbname);
    }
}
