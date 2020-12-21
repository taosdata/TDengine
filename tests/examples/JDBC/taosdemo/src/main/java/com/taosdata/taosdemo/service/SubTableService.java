package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.dao.SubTableMapper;
import com.taosdata.taosdemo.dao.SubTableMapperImpl;
import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.service.data.SubTableMetaGenerator;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SubTableService extends AbstractService {

    private SubTableMapper mapper;

    public SubTableService(DataSource datasource) {
        this.mapper = new SubTableMapperImpl(datasource);
    }

    public void createSubTable(SuperTableMeta superTableMeta, long numOfTables, String prefixOfTable, int numOfThreadsForCreate) {
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreadsForCreate);
        for (long i = 0; i < numOfTables; i++) {
            long tableIndex = i;
            executor.execute(() -> createSubTable(superTableMeta, prefixOfTable + (tableIndex + 1)));
        }
        executor.shutdown();
    }

    public void createSubTable(SuperTableMeta superTableMeta, String tableName) {
        // 构造数据
        SubTableMeta meta = SubTableMetaGenerator.generate(superTableMeta, tableName);
        createSubTable(meta);
    }

    // 创建一张子表，可以指定database，supertable，tablename，tag值
    public void createSubTable(SubTableMeta subTableMeta) {
        mapper.createUsingSuperTable(subTableMeta);
    }

    /*************************************************************************************************************************/
    // 插入：多线程，多表
    public int insert(List<SubTableValue> subTableValues, int threadSize, int frequency) {
        ExecutorService executor = Executors.newFixedThreadPool(threadSize);
        Future<Integer> future = executor.submit(() -> insert(subTableValues));
        executor.shutdown();
        //TODO：frequency
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

}
