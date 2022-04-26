package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.FieldValue;
import com.taosdata.taosdemo.domain.RowValue;
import com.taosdata.taosdemo.utils.DataGenerator;

import java.util.*;

public class FieldValueGenerator {

    public static Random random = new Random(System.currentTimeMillis());

    // 生成start到end的时间序列，时间戳为顺序，不含有乱序，field的value为随机生成
    public static List<RowValue> generate(long start, long end, long timeGap, List<FieldMeta> fieldMetaList) {
        List<RowValue> values = new ArrayList<>();

        for (long ts = start; ts < end; ts += timeGap) {
            List<FieldValue> fieldValues = new ArrayList<>();
            // timestamp
            fieldValues.add(new FieldValue(fieldMetaList.get(0).getName(), ts));
            // other values
            for (int fieldInd = 1; fieldInd < fieldMetaList.size(); fieldInd++) {
                FieldMeta fieldMeta = fieldMetaList.get(fieldInd);
                fieldValues.add(new FieldValue(fieldMeta.getName(), DataGenerator.randomValue(fieldMeta.getType())));
            }
            values.add(new RowValue(fieldValues));
        }
        return values;
    }

    // 生成start到end的时间序列，时间戳为顺序，含有乱序，rate为乱序的比例，range为乱序前跳范围，field的value为随机生成
    public static List<RowValue> disrupt(List<RowValue> values, int rate, long range) {
        long timeGap = (long) (values.get(1).getFields().get(0).getValue()) - (long) (values.get(0).getFields().get(0).getValue());
        int bugSize = values.size() * rate / 100;
        Set<Integer> bugIndSet = new HashSet<>();
        while (bugIndSet.size() < bugSize) {
            bugIndSet.add(random.nextInt(values.size()));
        }
        for (Integer bugInd : bugIndSet) {
            Long timestamp = (Long) values.get(bugInd).getFields().get(0).getValue();
            Long newTimestamp = timestamp - timeGap - random.nextInt((int) range);
            values.get(bugInd).getFields().get(0).setValue(newTimestamp);
        }

        return values;
    }
}
