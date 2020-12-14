package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.RowValue;
import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.utils.TimeStampUtil;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;

public class SubTableValueGenerator {

    public static List<SubTableValue> generate(List<SubTableMeta> subTableMetaList, int numOfRowsPerTable, long start, long timeGap) {
        return generate(subTableMetaList, 0, subTableMetaList.size(), numOfRowsPerTable, start, timeGap);
    }

    public static void disrupt(List<SubTableValue> subTableValueList, int rate, long range) {
        subTableValueList.stream().forEach((tableValue) -> {
            List<RowValue> values = tableValue.getValues();
            FieldValueGenerator.disrupt(values, rate, range);
        });
    }

    public static List<List<SubTableValue>> split(List<SubTableValue> subTableValueList, int numOfTables, int numOfTablesPerSQL, int numOfRowsPerTable, int numOfValuesPerSQL) {
        List<List<SubTableValue>> dataList = new ArrayList<>();
        if (numOfRowsPerTable < numOfValuesPerSQL)
            numOfValuesPerSQL = numOfRowsPerTable;
        if (numOfTables < numOfTablesPerSQL)
            numOfTablesPerSQL = numOfTables;
        //table
        for (int tableCnt = 0; tableCnt < numOfTables; ) {
            int tableSize = numOfTablesPerSQL;
            if (tableCnt + tableSize > numOfTables) {
                tableSize = numOfTables - tableCnt;
            }
            // row
            for (int rowCnt = 0; rowCnt < numOfRowsPerTable; ) {
                int rowSize = numOfValuesPerSQL;
                if (rowCnt + rowSize > numOfRowsPerTable) {
                    rowSize = numOfRowsPerTable - rowCnt;
                }
                // System.out.println("rowCnt: " + rowCnt + ", rowSize: " + rowSize + ", tableCnt: " + tableCnt + ", tableSize: " + tableSize);
                // split
                List<SubTableValue> blocks = subTableValueList.subList(tableCnt, tableCnt + tableSize);
                List<SubTableValue> newBlocks = new ArrayList<>();
                for (int i = 0; i < blocks.size(); i++) {
                    SubTableValue subTableValue = blocks.get(i);
                    SubTableValue newSubTableValue = new SubTableValue();
                    BeanUtils.copyProperties(subTableValue, newSubTableValue);
                    List<RowValue> values = subTableValue.getValues().subList(rowCnt, rowCnt + rowSize);
                    newSubTableValue.setValues(values);
                    newBlocks.add(newSubTableValue);
                }
                dataList.add(newBlocks);

                rowCnt += rowSize;
            }
            tableCnt += tableSize;
        }
        return dataList;
    }

    public static void main(String[] args) {
        split(null, 99, 10, 99, 10);
    }

    public static List<SubTableValue> generate(List<SubTableMeta> subTableMetaList, int tableCnt, int tableSize, int rowSize, long startTime, long timeGap) {
        List<SubTableValue> subTableValueList = new ArrayList<>();
        for (int i = 0; i < tableSize; i++) {
            SubTableMeta subTableMeta = subTableMetaList.get(tableCnt + i);
            SubTableValue subTableValue = new SubTableValue();
            subTableValue.setDatabase(subTableMeta.getDatabase());
            subTableValue.setName(subTableMeta.getName());
            subTableValue.setSupertable(subTableMeta.getSupertable());
            subTableValue.setTags(subTableMeta.getTags());
            TimeStampUtil.TimeTuple tuple = TimeStampUtil.range(startTime, timeGap, rowSize);
            List<RowValue> values = FieldValueGenerator.generate(tuple.start, tuple.end, tuple.timeGap, subTableMeta.getFields());
            subTableValue.setValues(values);
        }
        return subTableValueList;
    }
}
