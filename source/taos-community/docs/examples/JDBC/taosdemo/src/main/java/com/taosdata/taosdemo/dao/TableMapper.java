package com.taosdata.taosdemo.dao;

import com.taosdata.taosdemo.domain.TableMeta;
import com.taosdata.taosdemo.domain.TableValue;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TableMapper {

    // Create: Normal table
    void create(TableMeta tableMeta);

    // Insert: Multiple records into one table
    int insertOneTableMultiValues(TableValue values);

    // Insert: Multiple records into one table, specified columns
    int insertOneTableMultiValuesWithColumns(TableValue values);

    // Insert: Multiple records into multiple tables
    int insertMultiTableMultiValues(List<TableValue> tables);

    // Insert: Multiple records into multiple tables, specified columns
    int insertMultiTableMultiValuesWithColumns(List<TableValue> tables);
}
