package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void escapeSingleQuota() {
        String s = "'''''a\\'";
        String news = Utils.escapeSingleQuota(s);
        Assert.assertEquals("\\'\\'\\'\\'\\'a\\'", news);

        s = "\'''''a\\'";
        news = Utils.escapeSingleQuota(s);
        Assert.assertEquals("\\'\\'\\'\\'\\'a\\'", news);

        s = "\'\'\'\''a\\'";
        news = Utils.escapeSingleQuota(s);
        Assert.assertEquals("\\'\\'\\'\\'\\'a\\'", news);
    }
}