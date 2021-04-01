package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSUtilsTest {

    private String OS;

    @Test
    public void inWindows() {
        Assert.assertEquals(OS.indexOf("win") >= 0, OSUtils.isWindows());
    }

    @Test
    public void isMac() {
        Assert.assertEquals(OS.indexOf("mac") >= 0, OSUtils.isMac());
    }

    @Test
    public void isLinux() {
        Assert.assertEquals(OS.indexOf("nux") >= 0, OSUtils.isLinux());
    }

    @Before
    public void before() {
        OS = System.getProperty("os.name").toLowerCase();
    }
}