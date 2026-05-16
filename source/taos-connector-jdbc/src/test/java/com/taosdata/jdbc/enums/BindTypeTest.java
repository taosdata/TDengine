package com.taosdata.jdbc.enums;

import org.junit.Test;

import static org.junit.Assert.*;

public class BindTypeTest {

    @Test
    public void testGet() {
        assertEquals(1, BindType.TAG.get());
        assertEquals(2, BindType.BIND.get());
    }
}