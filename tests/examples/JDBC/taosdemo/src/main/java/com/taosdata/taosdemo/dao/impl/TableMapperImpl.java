package com.taosdata.taosdemo.dao.impl;

import com.taosdata.taosdemo.dao.TableMapper;
import com.taosdata.taosdemo.domain.TableMeta;
import com.taosdata.taosdemo.domain.TableValue;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class TableMapperImpl implements TableMapper {
    private JdbcTemplate template;

    @Override
    public void create(TableMeta tableMeta) {
        String sql = SqlSpeller.createTable(tableMeta);
        template.execute(sql);
    }

    @Override
    public int insertOneTableMultiValues(TableValue values) {
        String sql = SqlSpeller.insertOneTableMultiValues(values);
        return template.update(sql);
    }

    @Override
    public int insertOneTableMultiValuesWithColumns(TableValue values) {
        String sql = SqlSpeller.insertOneTableMultiValuesWithColumns(values);
        return template.update(sql);
    }

    @Override
    public int insertMultiTableMultiValues(List<TableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValues(tables);
        return template.update(sql);
    }

    @Override
    public int insertMultiTableMultiValuesWithColumns(List<TableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValuesWithColumns(tables);
        return template.update(sql);
    }
}
