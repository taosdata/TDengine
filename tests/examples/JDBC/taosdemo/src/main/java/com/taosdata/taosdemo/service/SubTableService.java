package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.mapper.SubTableMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class SubTableService extends AbstractService {

    @Autowired
    private SubTableMapper mapper;

    /**
     * 1. 选择database，找到所有supertable
     * 2. 选择supertable，可以拿到表结构，包括field和tag
     * 3. 指定子表的前缀和个数
     * 4. 指定创建子表的线程数
     */
    //TODO：指定database、supertable、子表前缀、子表个数、线程数

    // 多线程创建表，指定线程个数
    public int createSubTable(List<SubTableMeta> subTables, int threadSize) {
        ExecutorService executor = Executors.newFixedThreadPool(threadSize);
        List<Future<Integer>> futureList = new ArrayList<>();
        for (SubTableMeta subTableMeta : subTables) {
            Future<Integer> future = executor.submit(() -> createSubTable(subTableMeta));
            futureList.add(future);
        }
        executor.shutdown();
        return getAffectRows(futureList);
    }


    // 创建一张子表，可以指定database，supertable，tablename，tag值
    public int createSubTable(SubTableMeta subTableMeta) {
        return mapper.createUsingSuperTable(subTableMeta);
    }

    // 单线程创建多张子表，每张子表分别可以指定自己的database，supertable，tablename，tag值
    public int createSubTable(List<SubTableMeta> subTables) {
        return createSubTable(subTables, 1);
    }

    /*************************************************************************************************************************/
    // 插入：多线程，多表
    public int insert(List<SubTableValue> subTableValues, int threadSize) {
        ExecutorService executor = Executors.newFixedThreadPool(threadSize);
        Future<Integer> future = executor.submit(() -> insert(subTableValues));
        executor.shutdown();
        return getAffectRows(future);
    }

    // 插入：多线程，多表, 自动建表
    public int insertAutoCreateTable(List<SubTableValue> subTableValues, int threadSize) {
        ExecutorService executor = Executors.newFixedThreadPool(threadSize);
        Future<Integer> future = executor.submit(() -> insertAutoCreateTable(subTableValues));
        executor.shutdown();
        return getAffectRows(future);
    }

    // 插入：单表，insert into xxx values(),()...
    public int insert(SubTableValue subTableValue) {
        return mapper.insertOneTableMultiValues(subTableValue);
    }

    // 插入: 多表，insert into xxx values(),()... xxx values(),()...
    public int insert(List<SubTableValue> subTableValues) {
        return mapper.insertMultiTableMultiValuesUsingSuperTable(subTableValues);
    }

    // 插入：单表，自动建表, insert into xxx using xxx tags(...) values(),()...
    public int insertAutoCreateTable(SubTableValue subTableValue) {
        return mapper.insertOneTableMultiValuesUsingSuperTable(subTableValue);
    }

    // 插入：多表，自动建表, insert into xxx using XXX tags(...) values(),()... xxx using XXX tags(...) values(),()...
    public int insertAutoCreateTable(List<SubTableValue> subTableValues) {
        return mapper.insertMultiTableMultiValuesUsingSuperTable(subTableValues);
    }


//        ExecutorService executors = Executors.newFixedThreadPool(threadSize);
//        int count = 0;
//
//        //
//        List<SubTableValue> subTableValues = new ArrayList<>();
//        for (int tableIndex = 1; tableIndex <= numOfTablesPerSQL; tableIndex++) {
//            // each table
//            SubTableValue subTableValue = new SubTableValue();
//            subTableValue.setDatabase();
//            subTableValue.setName();
//            subTableValue.setSupertable();
//
//            List<RowValue> values = new ArrayList<>();
//            for (int valueCnt = 0; valueCnt < numOfValuesPerSQL; valueCnt++) {
//                List<FieldValue> fields = new ArrayList<>();
//                for (int fieldInd = 0; fieldInd <; fieldInd++) {
//                    FieldValue<Object> field = new FieldValue<>("", "");
//                    fields.add(field);
//                }
//                RowValue row = new RowValue();
//                row.setFields(fields);
//                values.add(row);
//            }
//            subTableValue.setValues(values);
//            subTableValues.add(subTableValue);
//        }


}
