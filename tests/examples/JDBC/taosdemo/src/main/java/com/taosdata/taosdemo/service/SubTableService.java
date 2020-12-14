package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.*;
import com.taosdata.taosdemo.mapper.SubTableMapper;
import com.taosdata.taosdemo.service.data.SubTableMetaGenerator;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Service
public class SubTableService extends AbstractService {

    private static Logger logger = Logger.getLogger(SubTableService.class);

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

    public void createSubTable(SuperTableMeta superTableMeta, int numOfTables, String prefixOfTable, int numOfThreadsForCreate) {
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreadsForCreate);
        for (int i = 0; i < numOfTables; i++) {
            int tableIndex = i;
            executor.execute(() -> createSubTable(superTableMeta, prefixOfTable + (tableIndex + 1)));
        }
        executor.shutdown();
    }

    public void createSubTable(SuperTableMeta superTableMeta, String tableName) {
        // 构造数据
        SubTableMeta meta = SubTableMetaGenerator.generate(superTableMeta, tableName);
        mapper.createUsingSuperTable(meta);
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
    public int insert(List<SubTableValue> subTableValues, int threadSize, int frequency) {
        ExecutorService executor = Executors.newFixedThreadPool(threadSize);
        Future<Integer> future = executor.submit(() -> insert(subTableValues));
        executor.shutdown();
        //TODO：frequency
        return getAffectRows(future);
    }

    // 插入：多线程，多表, 自动建表
    public int insertAutoCreateTable(List<SubTableValue> subTableValues, int threadSize, int frequency) {
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

    @Autowired
    private SqlSessionFactory sqlSessionFactory;
    @Autowired
    private DataSource dataSource;

    // 插入：多表，自动建表, insert into xxx using XXX tags(...) values(),()... xxx using XXX tags(...) values(),()...
    public int insertAutoCreateTable(List<SubTableValue> subTableValues) {
        Connection connection = null;
        Statement statement = null;
        int affectRows = 0;
        try {
            connection = dataSource.getConnection();
//            String sql = sqlSessionFactory.getConfiguration()
//                    .getMappedStatement("com.taosdata.taosdemo.mapper.SubTableMapper.insertMultiTableMultiValuesUsingSuperTable")
//                    .getBoundSql(subTableValues)
//                    .getSql();
            String sql = sql(subTableValues);
            logger.info(">>> SQL : " + sql);
            statement = connection.createStatement();
            affectRows = statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return affectRows;
//        return mapper.insertMultiTableMultiValuesUsingSuperTable(subTableValues);
    }

    private String sql(List<SubTableValue> subTableValues) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ");
        for (int i = 0; i < subTableValues.size(); i++) {
            SubTableValue subTableValue = subTableValues.get(i);
            sb.append(subTableValue.getDatabase() + "." + subTableValue.getName() + " using " + subTableValue.getSupertable() + " tags (");
            for (int j = 0; j < subTableValue.getTags().size(); j++) {
                TagValue tagValue = subTableValue.getTags().get(j);
                if (j == 0)
                    sb.append("'" + tagValue.getValue() + "'");
                else
                    sb.append(", '" + tagValue.getValue() + "'");
            }
            sb.append(") values");
            for (int j = 0; j < subTableValue.getValues().size(); j++) {
                sb.append("(");
                RowValue rowValue = subTableValue.getValues().get(j);
                for (int k = 0; k < rowValue.getFields().size(); k++) {
                    FieldValue fieldValue = rowValue.getFields().get(k);
                    if (k == 0)
                        sb.append("" + fieldValue.getValue() + "");
                    else
                        sb.append(", '" + fieldValue.getValue() + "'");
                }
                sb.append(") ");
            }
        }

        return sb.toString();
    }


    private static void sleep(int sleep) {
        if (sleep <= 0)
            return;
        try {
            TimeUnit.MILLISECONDS.sleep(sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /********************************************************************/


}
