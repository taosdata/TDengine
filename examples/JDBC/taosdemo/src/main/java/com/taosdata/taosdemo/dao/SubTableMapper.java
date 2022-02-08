package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SubTableMapper {

    // 创建：子表
    void createUsingSuperTable(SubTableMeta subTableMeta);

    // 插入：一张子表多个values
    int insertOneTableMultiValues(SubTableValue subTableValue);

    // 插入：一张子表多个values, 自动建表
    int insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue);

    // 插入：多张表多个values
    int insertMultiTableMultiValues(List<SubTableValue> tables);

    // 插入：多张表多个values，自动建表
    int insertMultiTableMultiValuesUsingSuperTable(List<SubTableValue> tables);

    //<!-- TODO:修改子表标签值 alter table ${tablename} set tag tagName=newTagValue-->

}