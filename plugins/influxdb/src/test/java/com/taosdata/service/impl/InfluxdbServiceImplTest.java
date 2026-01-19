package com.taosdata.service.impl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.springframework.test.context.junit.jupiter.SpringExtension;


@ExtendWith(SpringExtension.class)
public class InfluxdbServiceImplTest {

    @InjectMocks
    InfluxdbServiceImpl influxdbServiceImpl;

    @Test
    void testEscapeBackslash() {
        Assertions.assertEquals("=", influxdbServiceImpl.escapeBackslash("\\="));
        Assertions.assertEquals(",", influxdbServiceImpl.escapeBackslash("\\,"));
        Assertions.assertEquals(" ", influxdbServiceImpl.escapeBackslash("\\ "));
        Assertions.assertEquals("\\\\", influxdbServiceImpl.escapeBackslash("\\"));
        Assertions.assertEquals("\\\\=", influxdbServiceImpl.escapeBackslash("\\\\="));
        Assertions.assertEquals("\\\\ ", influxdbServiceImpl.escapeBackslash("\\\\ "));
        Assertions.assertEquals("z\\\\\"a", influxdbServiceImpl.escapeBackslash("z\\\"a"));
        Assertions.assertEquals("zgc abc", influxdbServiceImpl.escapeBackslash("zgc\\ abc"));
        Assertions.assertEquals("zgc\\\\ abc", influxdbServiceImpl.escapeBackslash("zgc\\\\ abc"));
    }
}
