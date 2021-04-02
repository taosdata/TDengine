package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

public class SubTableMapperImpl implements SubTableMapper {

    private static final Logger logger = Logger.getLogger(SubTableMapperImpl.class);
    private final JdbcTemplate jdbcTemplate;

    public SubTableMapperImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void createUsingSuperTable(SubTableMeta subTableMeta) {
        String sql = SqlSpeller.createTableUsingSuperTable(subTableMeta);
        logger.debug("SQL >>> " + sql);
        jdbcTemplate.execute(sql);
    }

    @Override
    public int insertOneTableMultiValues(SubTableValue subTableValue) {
        String sql = SqlSpeller.insertOneTableMultiValues(subTableValue);
        logger.debug("SQL >>> " + sql);

        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }

    @Override
    public int insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue) {
        String sql = SqlSpeller.insertOneTableMultiValuesUsingSuperTable(subTableValue);
        logger.debug("SQL >>> " + sql);

        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }

    @Override
    public int insertMultiTableMultiValues(List<SubTableValue> tables) {
        String sql = SqlSpeller.insertMultiSubTableMultiValues(tables);
        logger.debug("SQL >>> " + sql);
        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }

    @Override
    public int insertMultiTableMultiValuesUsingSuperTable(List<SubTableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValuesUsingSuperTable(tables);
        logger.debug("SQL >>> " + sql);
        int affectRows = 0;
        try {
            affectRows = jdbcTemplate.update(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectRows;
    }
}
