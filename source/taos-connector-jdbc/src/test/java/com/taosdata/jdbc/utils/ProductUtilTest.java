package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

public class ProductUtilTest {
    @Test
    public void testGetProductInfo() {
        Assert.assertNotNull(ProductUtil.getProductName());
        Assert.assertNotNull(ProductUtil.getProductVersion());
    }
}
