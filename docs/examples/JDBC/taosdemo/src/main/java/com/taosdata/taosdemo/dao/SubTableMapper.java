package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SubTableMapper {

    // Create: SubTable
    void createUsingSuperTable(SubTableMeta subTableMeta);

    // Insert: Multiple records into one SubTable
    int insertOneTableMultiValues(SubTableValue subTableValue);

    // Insert: Multiple records into one SubTable, auto create SubTables
    int insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue);

    // Insert: Multiple records into multiple SubTable
    int insertMultiTableMultiValues(List<SubTableValue> tables);

    // Insert: Multiple records into multiple SubTable, auto create SubTables
    int insertMultiTableMultiValuesUsingSuperTable(List<SubTableValue> tables);

    // <!-- TODO: Modify SubTable tag value: alter table ${tablename} set tag
    // tagName=newTagValue-->

}
