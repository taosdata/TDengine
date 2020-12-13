package com.taosdata.taosdemo.mapper;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SubTableMapper {

    // 创建：子表
    int createUsingSuperTable(SubTableMeta subTableMeta);

    // 插入：一张子表多个values
    int insertOneTableMultiValues(SubTableValue subTableValue);

    // 插入：一张子表多个values, 自动建表
    int insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue);

    // 插入：多张表多个values
    int insertMultiTableMultiValues(@Param("tables") List<SubTableValue> tables);

    // 插入：多张表多个values，自动建表
    int insertMultiTableMultiValuesUsingSuperTable(@Param("tables") List<SubTableValue> tables);

    //<!-- TODO:修改子表标签值 alter table ${tablename} set tag tagName=newTagValue-->

}