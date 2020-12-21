package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.dao.SuperTableMapper;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.springframework.jdbc.core.JdbcTemplate;

public class SuperTableMapperImpl implements SuperTableMapper {

    private JdbcTemplate jdbcTemplate;

    @Override
    public void createSuperTable(SuperTableMeta tableMetadata) {
        String sql = SqlSpeller.createSuperTable(tableMetadata);
        jdbcTemplate.execute(sql);
    }

    @Override
    public void dropSuperTable(String database, String name) {
        jdbcTemplate.execute("drop table if exists " + database + "." + name);
    }
}
