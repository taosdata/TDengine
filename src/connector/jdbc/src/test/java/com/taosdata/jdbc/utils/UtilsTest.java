package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Stream;

public class UtilsTest {

    @Test
    public void escapeSingleQuota() {
        // given
        String s = "'''''a\\'";
        // when
        String news = Utils.escapeSingleQuota(s);
        // then
        Assert.assertEquals("\\'\\'\\'\\'\\'a\\'", news);

        // given
        s = "'''''a\\'";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        Assert.assertEquals("\\'\\'\\'\\'\\'a\\'", news);

        // given
        s = "'''''a\\'";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        Assert.assertEquals("\\'\\'\\'\\'\\'a\\'", news);
    }

    @Test
    public void lowerCase() {
        // given
        String nativeSql = "insert into ?.? (ts, temperature, humidity) using ?.? tags(?,?) values(now, ?, ?)";
        Object[] parameters = Stream.of("test", "t1", "test", "weather", "beijing", 1, 12.2, 4).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "insert into test.t1 (ts, temperature, humidity) using test.weather tags('beijing',1) values(now, 12.2, 4)";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void upperCase() {
        // given
        String nativeSql = "INSERT INTO ? (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (?)  VALUES (?,?,?,?)";
        Object[] parameters = Stream.of("d1", 1, 123, 3.14, 220, 4).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT INTO d1 (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (1)  VALUES (123,3.14,220,4)";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void multiValues() {
        // given
        String nativeSql = "INSERT INTO ? (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (?)  VALUES (?,?,?,?),(?,?,?,?)";
        Object[] parameters = Stream.of("d1", 1, 100, 3.14, "abc", 4, 200, 3.1415, "xyz", 5).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT INTO d1 (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (1)  VALUES (100,3.14,'abc',4),(200,3.1415,'xyz',5)";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void lineTerminator() {
        // given
        String nativeSql = "INSERT INTO ? (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (?)  VALUES (?,?,\r\n?,?),(?,?,?,?)";
        Object[] parameters = Stream.of("d1", 1, 100, 3.14, "abc", 4, 200, 3.1415, "xyz", 5).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT INTO d1 (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (1)  VALUES (100,3.14,\r\n'abc',4),(200,3.1415,'xyz',5)";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void lineTerminatorAndMultiValues() {
        String nativeSql = "INSERT Into ? TAGS(?) VALUES(?,?,\r\n?,?),(?,? ,\r\n?,?) t? tags (?) Values (?,?,?\r\n,?),(?,?,?,?) t? Tags(?) values  (?,?,?,?) , (?,?,?,?)";
        Object[] parameters = Stream.of("t1", "abc", 100, 1.1, "xxx", "xxx", 200, 2.2, "xxx", "xxx", 2, "bcd", 300, 3.3, "xxx", "xxx", 400, 4.4, "xxx", "xxx", 3, "cde", 500, 5.5, "xxx", "xxx", 600, 6.6, "xxx", "xxx").toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT Into t1 TAGS('abc') VALUES(100,1.1,\r\n'xxx','xxx'),(200,2.2 ,\r\n'xxx','xxx') t2 tags ('bcd') Values (300,3.3,'xxx'\r\n,'xxx'),(400,4.4,'xxx','xxx') t3 Tags('cde') values  (500,5.5,'xxx','xxx') , (600,6.6,'xxx','xxx')";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void replaceNothing() {
        // given
        String nativeSql = "insert into test.t1 (ts, temperature, humidity) using test.weather tags('beijing',1) values(now, 12.2, 4)";

        // when
        String actual = Utils.getNativeSql(nativeSql, null);

        // then
        Assert.assertEquals(nativeSql, actual);
    }

    @Test
    public void replaceNothing2() {
        // given
        String nativeSql = "insert into test.t1 (ts, temperature, humidity) using test.weather tags('beijing',1) values(now, 12.2, 4)";
        Object[] parameters = Stream.of("test", "t1", "test", "weather", "beijing", 1, 12.2, 4).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        Assert.assertEquals(nativeSql, actual);
    }

    @Test
    public void replaceNothing3() {
        // given
        String nativeSql = "insert into ?.? (ts, temperature, humidity) using ?.? tags(?,?) values(now, ?, ?)";

        // when
        String actual = Utils.getNativeSql(nativeSql, null);

        // then
        Assert.assertEquals(nativeSql, actual);

    }
}