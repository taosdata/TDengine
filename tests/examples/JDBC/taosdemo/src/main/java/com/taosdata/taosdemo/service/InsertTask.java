package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.service.data.SubTableValueGenerator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;

public class InsertTask implements Callable<Integer> {

    private final long startTableInd; // included
    private final long endTableInd;   // excluded
    private final long startTime;
    private final long timeGap;
    private final long numOfRowsPerTable;
    private long numOfTablesPerSQL;
    private long numOfValuesPerSQL;
    private final SuperTableMeta superTableMeta;
    private final int order;
    private final int rate;
    private final long range;
    private final String prefixOfTable;
    private final boolean autoCreateTable;
    private final DataSource dataSource;

    public InsertTask(SuperTableMeta superTableMeta, long startTableInd, long endTableInd,
                      long startTime, long timeGap,
                      long numOfRowsPerTable, long numOfTablesPerSQL, long numOfValuesPerSQL,
                      int order, int rate, long range,
                      String prefixOfTable, boolean autoCreateTable, DataSource dataSource) {
        this.superTableMeta = superTableMeta;
        this.startTableInd = startTableInd;
        this.endTableInd = endTableInd;
        this.startTime = startTime;
        this.timeGap = timeGap;
        this.numOfRowsPerTable = numOfRowsPerTable;
        this.numOfTablesPerSQL = numOfTablesPerSQL;
        this.numOfValuesPerSQL = numOfValuesPerSQL;
        this.order = order;
        this.rate = rate;
        this.range = range;
        this.prefixOfTable = prefixOfTable;
        this.autoCreateTable = autoCreateTable;
        this.dataSource = dataSource;
    }


    @Override
    public Integer call() throws Exception {

        Connection connection = dataSource.getConnection();

        long numOfTables = endTableInd - startTableInd;
        if (numOfRowsPerTable < numOfValuesPerSQL)
            numOfValuesPerSQL = (int) numOfRowsPerTable;
        if (numOfTables < numOfTablesPerSQL)
            numOfTablesPerSQL = (int) numOfTables;

        // row
        for (long rowCnt = 0; rowCnt < numOfRowsPerTable; ) {
            long rowSize = numOfValuesPerSQL;
            if (rowCnt + rowSize > numOfRowsPerTable) {
                rowSize = numOfRowsPerTable - rowCnt;
            }
            //table
            for (long tableCnt = startTableInd; tableCnt < endTableInd; ) {
                long tableSize = numOfTablesPerSQL;
                if (tableCnt + tableSize > endTableInd) {
                    tableSize = endTableInd - tableCnt;
                }
                long startTime = this.startTime + rowCnt * timeGap;

//                System.out.println(Thread.currentThread().getName() + " >>> " + "rowCnt: " + rowCnt + ", rowSize: " + rowSize + ", " + "tableCnt: " + tableCnt + ",tableSize: " + tableSize + ", " + "startTime: " + startTime + ",timeGap: " + timeGap + "");
                /***********************************************/
                // 生成数据
                List<SubTableValue> data = SubTableValueGenerator.generate(superTableMeta, prefixOfTable, tableCnt, tableSize, rowSize, startTime, timeGap);
                // 乱序
                if (order != 0) {
                    SubTableValueGenerator.disrupt(data, rate, range);
                }
                // insert
                SubTableService subTableService = new SubTableService(dataSource);
                if (autoCreateTable) {
                    subTableService.insertAutoCreateTable(data);
                } else {
                    subTableService.insert(data);
                }
                /***********************************************/
                tableCnt += tableSize;
            }
            rowCnt += rowSize;
        }

        connection.close();
        return 1;
    }

}
