package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.dao.SubTableMapper;
import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class SubTableMapperImpl implements SubTableMapper {

    private JdbcTemplate jdbcTemplate;

    @Override
    public void createUsingSuperTable(SubTableMeta subTableMeta) {
        String sql = SqlSpeller.createTableUsingSuperTable(subTableMeta);
        jdbcTemplate.execute(sql);
    }

    @Override
    public int insertOneTableMultiValues(SubTableValue subTableValue) {
        String sql = SqlSpeller.insertOneTableMultiValues(subTableValue);
        return jdbcTemplate.update(sql);
    }


    @Override
    public int insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue) {
        String sql = SqlSpeller.insertOneTableMultiValuesUsingSuperTable(subTableValue);
        return jdbcTemplate.update(sql);
    }

    @Override
    public int insertMultiTableMultiValues(List<SubTableValue> tables) {
        String sql = SqlSpeller.insertMultiSubTableMultiValues(tables);
        return jdbcTemplate.update(sql);
    }

    @Override
    public int insertMultiTableMultiValuesUsingSuperTable(List<SubTableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValuesUsingSuperTable(tables);
        return jdbcTemplate.update(sql);
    }
}
