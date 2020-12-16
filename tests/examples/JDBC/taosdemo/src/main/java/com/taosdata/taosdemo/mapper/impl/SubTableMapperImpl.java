package com.taosdata.taosdemo.mapper.impl;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.mapper.SubTableMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class SubTableMapperImpl implements SubTableMapper {

    private JdbcTemplate jdbcTemplate;

    @Override
    public int createUsingSuperTable(SubTableMeta subTableMeta) {
        return 0;
    }

    @Override
    public int insertOneTableMultiValues(SubTableValue subTableValue) {
        return 0;
    }

    @Override
    public int insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue) {
        return 0;
    }

    @Override
    public int insertMultiTableMultiValues(List<SubTableValue> tables) {
        return 0;
    }

    @Override
    public int insertMultiTableMultiValuesUsingSuperTable(List<SubTableValue> tables) {
        return 0;
    }
}
