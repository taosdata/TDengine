package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBConstants;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class UtilsTest {

    @Test
    public void escapeSingleQuota() {
        // given
        String s = "'''''a\\'";
        // when
        String news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("\\'\\'\\'\\'\\'a\\'", news);

        // given
        s = "abc";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("abc", news);

        // given
        s = "\\a\\\\b\\\\c\\\\\\";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("\\a\\\\b\\\\c\\\\\\", news);

        // given
        s = "abc'";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("abc\\'", news);

        // given
        s = "a'bc";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("a\\'bc", news);

        // given
        s = "'abc";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("\\'abc", news);

        // given
        s = "'''a'''b'''c'''";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("\\'\\'\\'a\\'\\'\\'b\\'\\'\\'c\\'\\'\\'", news);

        // given
        s = "'''a\\'\\'\\'b'''c\\'\\'\\'";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("\\'\\'\\'a\\'\\'\\'b\\'\\'\\'c\\'\\'\\'", news);

        // given
        s = "'''a\\\\'\\'\\\\'b'''c\\'\\'\\\\'";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("\\'\\'\\'a\\\\'\\'\\\\'b\\'\\'\\'c\\'\\'\\\\'", news);

        // given
        s = "\\'\\'\\'a'''b'''c\\'\\'\\'";
        // when
        news = Utils.escapeSingleQuota(s);
        // then
        assertEquals("\\'\\'\\'a\\'\\'\\'b\\'\\'\\'c\\'\\'\\'", news);
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
        assertEquals(expected, actual);
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
        assertEquals(expected, actual);
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
        assertEquals(expected, actual);
    }

    @Test
    public void multiValuesAndWhitespace() {
        // given
        String nativeSql = "INSERT INTO ? (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (?)  VALUES (?,?,?,?)  (?,?,?,?) (?,?,?,?)";
        Object[] parameters = Stream.of("d1", 1, 100, 3.14, "abc", 4, 200, 3.1415, "xyz", 5, 300, 3.141592, "uvw", 6).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT INTO d1 (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (1)  VALUES (100,3.14,'abc',4)  (200,3.1415,'xyz',5) (300,3.141592,'uvw',6)";
        assertEquals(expected, actual);
    }

    @Test
    public void multiValuesNoSeparator() {
        // given
        String nativeSql = "INSERT INTO ? (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (?)  VALUES (?,?,?,?)(?,?,?,?)(?,?,?,?)";
        Object[] parameters = Stream.of("d1", 1, 100, 3.14, "abc", 4, 200, 3.1415, "xyz", 5, 300, 3.141592, "uvw", 6).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT INTO d1 (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (1)  VALUES (100,3.14,'abc',4)(200,3.1415,'xyz',5)(300,3.141592,'uvw',6)";
        assertEquals(expected, actual);
    }

    @Test
    public void multiValuesMultiSeparator() {
        // given
        String nativeSql = "INSERT INTO ? (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (?)  VALUES (?,?,?,?) (?,?,?,?), (?,?,?,?)";
        Object[] parameters = Stream.of("d1", 1, 100, 3.14, "abc", 4, 200, 3.1415, "xyz", 5, 300, 3.141592, "uvw", 6).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT INTO d1 (TS,CURRENT,VOLTAGE,PHASE) USING METERS TAGS (1)  VALUES (100,3.14,'abc',4) (200,3.1415,'xyz',5), (300,3.141592,'uvw',6)";
        assertEquals(expected, actual);
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
        assertEquals(expected, actual);
    }

    @Test
    public void lineTerminatorAndMultiValues() {
        String nativeSql = "INSERT Into ? TAGS(?) VALUES(?,?,\r\n?,?),(?,? ,\r\n?,?) t? tags (?) Values (?,?,?\r\n,?),(?,?,?,?) t? Tags(?) values  (?,?,?,?) , (?,?,?,?)";
        Object[] parameters = Stream.of("t1", "abc", 100, 1.1, "xxx", "xxx", 200, 2.2, "xxx", "xxx", 2, "bcd", 300, 3.3, "xxx", "xxx", 400, 4.4, "xxx", "xxx", 3, "cde", 500, 5.5, "xxx", "xxx", 600, 6.6, "xxx", "xxx").toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT Into t1 TAGS('abc') VALUES(100,1.1,\r\n'xxx','xxx'),(200,2.2 ,\r\n'xxx','xxx') t2 tags ('bcd') Values (300,3.3,'xxx'\r\n,'xxx'),(400,4.4,'xxx','xxx') t3 Tags('cde') values  (500,5.5,'xxx','xxx') , (600,6.6,'xxx','xxx')";
        assertEquals(expected, actual);
    }

    @Test
    public void lineTerminatorAndMultiValuesAndNoneOrMoreWhitespace() {
        String nativeSql = "INSERT Into ? TAGS(?) VALUES(?,?,\r\n?,?),(?,? ,\r\n?,?) t? tags (?) Values (?,?,?\r\n,?) (?,?,?,?) t? Tags(?) values  (?,?,?,?) , (?,?,?,?)";
        Object[] parameters = Stream.of("t1", "abc", 100, 1.1, "xxx", "xxx", 200, 2.2, "xxx", "xxx", 2, "bcd", 300, 3.3, "xxx", "xxx", 400, 4.4, "xxx", "xxx", 3, "cde", 500, 5.5, "xxx", "xxx", 600, 6.6, "xxx", "xxx").toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT Into t1 TAGS('abc') VALUES(100,1.1,\r\n'xxx','xxx'),(200,2.2 ,\r\n'xxx','xxx') t2 tags ('bcd') Values (300,3.3,'xxx'\r\n,'xxx') (400,4.4,'xxx','xxx') t3 Tags('cde') values  (500,5.5,'xxx','xxx') , (600,6.6,'xxx','xxx')";
        assertEquals(expected, actual);
    }

    @Test
    public void multiValuesAndNoneOrMoreWhitespace() {
        String nativeSql = "INSERT INTO ? USING traces TAGS (?, ?) VALUES (?, ?, ?, ?, ?, ?, ?)  (?, ?, ?, ?, ?, ?, ?)";
        Object[] parameters = Stream.of("t1", "t1", "t2", 1632968284000L, 111.111, 119.001, 0.4, 90, 99.1, "WGS84", 1632968285000L, 111.21109999999999, 120.001, 0.5, 91, 99.19999999999999, "WGS84").toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        String expected = "INSERT INTO t1 USING traces TAGS ('t1', 't2') VALUES (1632968284000, 111.111, 119.001, 0.4, 90, 99.1, 'WGS84')  (1632968285000, 111.21109999999999, 120.001, 0.5, 91, 99.19999999999999, 'WGS84')";
        assertEquals(expected, actual);
    }

    @Test
    public void replaceNothing() {
        // given
        String nativeSql = "insert into test.t1 (ts, temperature, humidity) using test.weather tags('beijing',1) values(now, 12.2, 4)";

        // when
        String actual = Utils.getNativeSql(nativeSql, null);

        // then
        assertEquals(nativeSql, actual);
    }

    @Test
    public void replaceNothing2() {
        // given
        String nativeSql = "insert into test.t1 (ts, temperature, humidity) using test.weather tags('beijing',1) values(now, 12.2, 4)";
        Object[] parameters = Stream.of("test", "t1", "test", "weather", "beijing", 1, 12.2, 4).toArray();

        // when
        String actual = Utils.getNativeSql(nativeSql, parameters);

        // then
        assertEquals(nativeSql, actual);
    }

    @Test
    public void replaceNothing3() {
        // given
        String nativeSql = "insert into ?.? (ts, temperature, humidity) using ?.? tags(?,?) values(now, ?, ?)";

        // when
        String actual = Utils.getNativeSql(nativeSql, null);

        // then
        assertEquals(nativeSql, actual);

    }

    @Test
    public void ubigintTest() {
        BigInteger bigInteger = new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG);
        long v = bigInteger.longValue();
        short b = (short)(v & 0xFF);
        assertEquals(255, b);
    }

    @Test
    public void testCompareVersions() throws SQLException {
        // Major version comparison
        assertEquals(1, VersionUtil.compareVersions("4.0.0", "3.0.0"));
        assertEquals(-1, VersionUtil.compareVersions("2.0.0", "3.0.0"));

        // Minor version comparison
        assertEquals(1, VersionUtil.compareVersions("3.1.0", "3.0.0"));
        assertEquals(-1, VersionUtil.compareVersions("3.0.0", "3.1.0"));

        // Patch version comparison
        assertEquals(1, VersionUtil.compareVersions("3.0.1", "3.0.0"));
        assertEquals(-1, VersionUtil.compareVersions("3.0.0", "3.0.1"));

        // Build number comparison
        assertEquals(1, VersionUtil.compareVersions("3.0.0.1", "3.0.0.0"));
        assertEquals(-1, VersionUtil.compareVersions("3.0.0.0", "3.0.0.1"));

        // Pre-release version comparison
        assertEquals(1, VersionUtil.compareVersions("3.0.0", "3.0.0-alpha"));
        assertEquals(-1, VersionUtil.compareVersions("3.0.0-alpha", "3.0.0"));
        assertEquals(1, VersionUtil.compareVersions("3.0.0-beta", "3.0.0-alpha"));
        assertEquals(-1, VersionUtil.compareVersions("3.0.0-alpha", "3.0.0-beta"));

        // Different version length comparison
        Assert.assertTrue( VersionUtil.compareVersions("3.3.6.0.0613", "3.3.6.0") > 0);
        Assert.assertTrue(VersionUtil.compareVersions("3.3.6.0", "3.3.6.0.0613") < 0);

        // Equal versions comparison
        assertEquals(0, VersionUtil.compareVersions("3.3.6.0", "3.3.6.0"));
        assertEquals(0, VersionUtil.compareVersions("3.3.6.0.0613", "3.3.6.0.0613"));
        assertEquals(0, VersionUtil.compareVersions("3.3.6.5-alpha", "3.3.6.5-alpha"));
    }

    @Test
    public void testUnescapeWithValidUnicode() {
        // Test valid Unicode escape sequences
        String input = "delete from `act1` where `ts` \\u003e= 1756792428951 and `ts` \\u003c= 1756792428951";
        String expected = "delete from `act1` where `ts` >= 1756792428951 and `ts` <= 1756792428951";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testUnescapeWithNoUnicode() {
        // Test string without Unicode escape sequences
        String input = "This is a normal string without any unicode escapes";
        String expected = "This is a normal string without any unicode escapes";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testUnescapeWithInvalidUnicode() {
        // Test invalid Unicode escape sequences
        String input = "Invalid unicode: \\u003z";
        String expected = "Invalid unicode: \\u003z";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testUnescapeWithIncompleteUnicode() {
        // Test incomplete Unicode escape sequences
        String input = "Incomplete unicode: \\u003";
        String expected = "Incomplete unicode: \\u003";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testUnescapeWithEmptyString() {
        // Test empty string
        String input = "";
        String expected = "";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testUnescapeWithUnicodeAtStartAndEnd() {
        // Test Unicode escape sequences at start and end of string
        String input = "\\u003cstart with unicode and end with unicode\\u003e";
        String expected = "<start with unicode and end with unicode>";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testUnescapeWithNonAsciiCharacters() {
        // Test non-ASCII Unicode escape sequences
        String input = "\\u4e2d\\u6587 Chinese";
        String expected = "中文 Chinese";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testUnescapeWithBackslashButNotUnicode() {
        // Test backslash that is not part of Unicode escape sequence
        String input = "This has a backslash\\ but not unicode";
        String expected = "This has a backslash\\ but not unicode";
        String actual = Utils.unescapeUnicode(input);
        assertEquals(expected, actual);
    }
}