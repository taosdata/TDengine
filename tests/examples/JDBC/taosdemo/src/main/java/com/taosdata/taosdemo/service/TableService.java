package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.TableMeta;
import com.taosdata.taosdemo.mapper.TableMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class TableService extends AbstractService {

    @Autowired
    private TableMapper tableMapper;

    //创建一张表
    public int create(TableMeta tableMeta) {
        return tableMapper.create(tableMeta);
    }

    //创建多张表
    public int create(List<TableMeta> tables) {
        return create(tables, 1);
    }

    //多线程创建多张表
    public int create(List<TableMeta> tables, int threadSize) {
        ExecutorService executors = Executors.newFixedThreadPool(threadSize);
        List<Future<Integer>> futures = new ArrayList<>();
        for (TableMeta table : tables) {
            Future<Integer> future = executors.submit(() -> create(table));
            futures.add(future);
        }
        return getAffectRows(futures);
    }


}
