package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.RowValue;
import com.taosdata.taosdemo.utils.TimeStampUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldValueGeneratorTest {

    private List<RowValue> rowValues;

    @Test
    public void generate() {
        List<FieldMeta> fieldMetas = new ArrayList<>();
        fieldMetas.add(new FieldMeta("ts", "timestamp"));
        fieldMetas.add(new FieldMeta("temperature", "float"));
        fieldMetas.add(new FieldMeta("humidity", "int"));

        long start = TimeStampUtil.datetimeToLong("2020-01-01 00:00:00.000");
        long end = TimeStampUtil.datetimeToLong("2020-01-01 10:00:00.000");

        rowValues = FieldValueGenerator.generate(start, end, 1000l * 3600, fieldMetas);
        Assert.assertEquals(10, rowValues.size());
    }

    @Test
    public void disrupt() {
        List<FieldMeta> fieldMetas = new ArrayList<>();
        fieldMetas.add(new FieldMeta("ts", "timestamp"));
        fieldMetas.add(new FieldMeta("temperature", "float"));
        fieldMetas.add(new FieldMeta("humidity", "int"));

        long start = TimeStampUtil.datetimeToLong("2020-01-01 00:00:00.000");
        long end = TimeStampUtil.datetimeToLong("2020-01-01 10:00:00.000");

        rowValues = FieldValueGenerator.generate(start, end, 1000l * 3600l, fieldMetas);

        FieldValueGenerator.disrupt(rowValues, 20, 1000);
        Assert.assertEquals(10, rowValues.size());
    }

    @After
    public void after() {
        for (RowValue row : rowValues) {
            row.getFields().stream().forEach(field -> {
                if (field.getName().equals("ts")) {
                    System.out.print(TimeStampUtil.longToDatetime((Long) field.getValue()));
                } else
                    System.out.print(" ," + field.getValue());
            });
            System.out.println();
        }
    }
}