package com.taosdata.taosdemo.utils;

import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorTest {

    @Test
    public void randomValue() {
        for (int i = 0; i < TaosConstants.DATA_TYPES.length; i++) {
            System.out.println(TaosConstants.DATA_TYPES[i] + " >>> " + DataGenerator.randomValue(TaosConstants.DATA_TYPES[i]));
        }
    }

    @Test
    public void randomNchar() {
        String s = DataGenerator.randomNchar(10);
        Assert.assertEquals(10, s.length());
    }
}