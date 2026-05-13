package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSUtilsTest {

    private String OS;

    @Test
    public void inWindows() {
        Assert.assertEquals(OS.contains("win"), OSUtils.isWindows());
    }

    @Test
    public void isMac() {
        Assert.assertEquals(OS.contains("mac"), OSUtils.isMac());
    }

    @Test
    public void isLinux() {
        Assert.assertEquals(OS.contains("nux"), OSUtils.isLinux());
    }

    @Before
    public void before() {
        OS = System.getProperty("os.name").toLowerCase();
    }
}