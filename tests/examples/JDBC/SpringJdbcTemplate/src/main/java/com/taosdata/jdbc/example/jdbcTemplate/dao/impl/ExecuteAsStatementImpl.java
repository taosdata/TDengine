package com.taosdata.jdbc.example.jdbcTemplate.dao.impl;

import com.taosdata.jdbc.example.jdbcTemplate.dao.ExecuteAsStatement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class ExecuteAsStatementImpl implements ExecuteAsStatement {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void doExecute(String sql) {
        jdbcTemplate.execute(sql);
    }
}
