package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class SuperTableMapperImpl implements SuperTableMapper {
    private static final Logger logger = Logger.getLogger(SuperTableMapperImpl.class);
    private JdbcTemplate jdbcTemplate;

    public SuperTableMapperImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void createSuperTable(SuperTableMeta tableMetadata) {
        String sql = SqlSpeller.createSuperTable(tableMetadata);
        logger.info("SQL >>> " + sql);
        jdbcTemplate.execute(sql);
    }

    @Override
    public void dropSuperTable(String database, String name) {
        String sql = "drop table if exists " + database + "." + name;
        logger.info("SQL >>> " + sql);
        jdbcTemplate.execute(sql);
    }
}
