package com.taosdata.tsync.utils;

import com.taosdata.tsync.entity.Person;
import org.junit.Test;

public class SchemaGeneratorTest {

    @Test
    public void test() {
        String schema = SchemaGenerator.build(Person.class);
//        String expected = "{\"namespace\":\"com.taosdata.tsync.domain\",\"name\":\"Person\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"age\",\"type\":[\"int\",\"null\"]}]}";
        System.out.println(schema);
//        Assert.assertEquals(expected,schema);
    }

}