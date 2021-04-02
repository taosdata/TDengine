package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.TableMeta;
import com.taosdata.taosdemo.domain.TableValue;
import com.taosdata.taosdemo.utils.SqlSpeller;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class TableMapperImpl implements TableMapper {
    private static final Logger logger = Logger.getLogger(TableMapperImpl.class);
    private JdbcTemplate template;

    @Override
    public void create(TableMeta tableMeta) {
        String sql = SqlSpeller.createTable(tableMeta);
        logger.debug("SQL >>> " + sql);
        template.execute(sql);
    }

    @Override
    public int insertOneTableMultiValues(TableValue values) {
        String sql = SqlSpeller.insertOneTableMultiValues(values);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }

    @Override
    public int insertOneTableMultiValuesWithColumns(TableValue values) {
        String sql = SqlSpeller.insertOneTableMultiValuesWithColumns(values);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }

    @Override
    public int insertMultiTableMultiValues(List<TableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValues(tables);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }

    @Override
    public int insertMultiTableMultiValuesWithColumns(List<TableValue> tables) {
        String sql = SqlSpeller.insertMultiTableMultiValuesWithColumns(tables);
        logger.debug("SQL >>> " + sql);
        return template.update(sql);
    }
}
