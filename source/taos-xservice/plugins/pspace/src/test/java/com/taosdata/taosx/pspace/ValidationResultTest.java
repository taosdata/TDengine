package com.taosdata.taosx.pspace;

import org.junit.Test;

import com.google.gson.Gson;

import static org.junit.Assert.*;

public class ValidationResultTest {

    @Test
    public void testJson() {
        CheckResult result = new CheckResult();

        result.setValid(true);
        result.setSupport(true);
        result.setDataSource("pspace");
        result.setVersion("7.1");

        Gson gson = new Gson();
        String json = gson.toJson(result);

        System.out.println(json);
        String expectedJson = "{\"valid\":true,\"support\":true,\"data_source\":\"pspace\",\"version\":\"7.1\"}";
        assertEquals(expectedJson, json);
    }
}
