package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.utils.SqlSpeller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;

public class DatabaseMapperImpl implements DatabaseMapper {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseMapperImpl.class);

    private final JdbcTemplate jdbcTemplate;

    public DatabaseMapperImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


    @Override
    public void createDatabase(String dbname) {
        String sql = "create database if not exists " + dbname;
        jdbcTemplate.execute(sql);
        logger.debug("SQL >>> " + sql);
    }

    @Override
    public void dropDatabase(String dbname) {
        String sql = "drop database if exists " + dbname;
        jdbcTemplate.update(sql);
        logger.debug("SQL >>> " + sql);
    }

    @Override
    public void createDatabaseWithParameters(Map<String, String> map) {
        String sql = SqlSpeller.createDatabase(map);
        jdbcTemplate.execute(sql);
        logger.debug("SQL >>> " + sql);
    }

    @Override
    public void useDatabase(String dbname) {
        String sql = "use " + dbname;
        jdbcTemplate.execute(sql);
        logger.debug("SQL >>> " + sql);
    }
}
