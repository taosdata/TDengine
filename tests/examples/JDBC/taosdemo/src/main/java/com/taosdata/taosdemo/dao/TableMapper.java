package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.TableMeta;
import com.taosdata.taosdemo.domain.TableValue;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TableMapper {

    // 创建：普通表
    void create(TableMeta tableMeta);

    // 插入：一张表多个value
    int insertOneTableMultiValues(TableValue values);

    // 插入: 一张表多个value，指定的列
    int insertOneTableMultiValuesWithColumns(TableValue values);

    // 插入：多个表多个value
    int insertMultiTableMultiValues(List<TableValue> tables);

    // 插入：多个表多个value, 指定的列
    int insertMultiTableMultiValuesWithColumns(List<TableValue> tables);

}