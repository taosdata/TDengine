package com.taosdata.taosdemo.mapper;

import com.taosdata.taosdemo.domain.TableMeta;
import com.taosdata.taosdemo.domain.TableValue;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TableMapper {

    // 创建：普通表
    int create(TableMeta tableMeta);

    // 插入：一张表多个value
    int insertOneTableMultiValues(TableValue values);

    // 插入: 一张表多个value，指定的列
    int insertOneTableMultiValuesWithColumns(TableValue values);

    // 插入：多个表多个value
    int insertMultiTableMultiValues(@Param("tables") List<TableValue> tables);

    // 插入：多个表多个value, 指定的列
    int insertMultiTableMultiValuesWithColumns(@Param("tables") List<TableValue> tables);

}