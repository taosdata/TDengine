package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.utils.SqlSpeller;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Map;

public class DatabaseMapperImpl implements DatabaseMapper {

    private final JdbcTemplate jdbcTemplate;

    public DatabaseMapperImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


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
