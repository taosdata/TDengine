package com.taosdata.jdbc;

import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(CatalogRunner.class)
@TestTarget(alias = "JsonTag", author = "huolibo", version = "2.0.36")
public class JsonTagTest {
    private static final String dbName = "json_tag_test";
    private static Connection connection;
    private static Statement statement;
    private static final String superSql = "create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)";
    private static final String[] sql = {
            "insert into jsons1_1 using jsons1 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(now, 1, false, 'json1', '你是') (1591060608000, 23, true, '等等', 'json')",
            "insert into jsons1_2 using jsons1 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060628000, 2, true, 'json2', 'sss')",
            "insert into jsons1_3 using jsons1 tags('{\"tag1\":false,\"tag2\":\"beijing\"}') values (1591060668000, 3, false, 'json3', 'efwe')",
            "insert into jsons1_4 using jsons1 tags('{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}') values (1591060728000, 4, true, 'json4', '323sd')",
            "insert into jsons1_5 using jsons1 tags('{\"tag1\":1.232, \"tag2\":null}') values(1591060928000, 1, false, '你就会', 'ewe')",
            "insert into jsons1_6 using jsons1 tags('{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}') values(1591061628000, 11, false, '你就会','')",
            "insert into jsons1_7 using jsons1 tags('{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '你就会', 'dws')",
            // test duplicate key using the first one.
            "CREATE TABLE if not exists jsons1_8 using jsons1 tags('{\"tag1\":null, \"tag1\":true, \"tag1\":45, \"1tag$\":2, \" \":90}')",

    };

    private static final String[] invalidJsonInsertSql = {
            // test empty json string, save as tag is NULL
            "insert into jsons1_9  using jsons1 tags('\t') values (1591062328000, 24, NULL, '你就会', '2sdw')",
    };

    private static final String[] invalidJsonCreateSql = {
            "CREATE TABLE if not exists jsons1_10 using jsons1 tags('')",
            "CREATE TABLE if not exists jsons1_11 using jsons1 tags(' ')",
            "CREATE TABLE if not exists jsons1_12 using jsons1 tags('{}')",
            "CREATE TABLE if not exists jsons1_13 using jsons1 tags('null')",
    };

    // test invalidate json
    private static final String[] errorJsonInsertSql = {
            "CREATE TABLE if not exists jsons1_14 using jsons1 tags('\"efwewf\"')",
            "CREATE TABLE if not exists jsons1_14 using jsons1 tags('3333')",
            "CREATE TABLE if not exists jsons1_14 using jsons1 tags('33.33')",
            "CREATE TABLE if not exists jsons1_14 using jsons1 tags('false')",
            "CREATE TABLE if not exists jsons1_14 using jsons1 tags('[1,true]')",
            "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{222}')",
            "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"fe\"}')",
    };

    private static final String[] errorSelectSql = {
            "select * from jsons1 where jtag->tag1='beijing'",
            "select * from jsons1 where jtag->'location'",
            "select * from jsons1 where jtag->''",
            "select * from jsons1 where jtag->''=9",
            "select -> from jsons1",
            "select ? from jsons1",
            "select * from jsons1 where contains",
            "select * from jsons1 where jtag->",
            "select jtag->location from jsons1",
            "select jtag contains location from jsons1",
            "select * from jsons1 where jtag contains location",
            "select * from jsons1 where jtag contains ''",
            "select * from jsons1 where jtag contains 'location'='beijing'",
            // test where with json tag
            "select * from jsons1_1 where jtag is not null",
            "select * from jsons1 where jtag='{\"tag1\":11,\"tag2\":\"\"}'",
            "select * from jsons1 where jtag->'tag1'={}"
    };

    @Test
    @Description("insert json tag")
    public void case01_InsertTest() throws SQLException {
        for (String sql : sql) {
            statement.execute(sql);
        }
        for (String sql : invalidJsonInsertSql) {
            statement.execute(sql);
        }
        for (String sql : invalidJsonCreateSql) {
            statement.execute(sql);
        }
    }

    @Test
    @Description("error json tag insert")
    public void case02_ErrorJsonInsertTest() {
        int count = 0;
        for (String sql : errorJsonInsertSql) {
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                count++;
            }
        }
        Assert.assertEquals(errorJsonInsertSql.length, count);
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when json value is array")
    public void case02_ArrayErrorTest() throws SQLException {
        statement.execute("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":[1,true]}')");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when json value is empty")
    public void case02_EmptyValueErrorTest() throws SQLException {
        statement.execute("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":{}}')");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when json key is not ASCII")
    public void case02_AbnormalKeyErrorTest1() throws SQLException {
        statement.execute("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"。loc\":\"fff\"}')");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when json key is '\\t'")
    public void case02_AbnormalKeyErrorTest2() throws SQLException {
        statement.execute("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\t\":\"fff\"}')");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when json key is chinese")
    public void case02_AbnormalKeyErrorTest3() throws SQLException {
        statement.execute("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"试试\":\"fff\"}')");
    }

    @Test
    @Description("alter json tag")
    public void case03_AlterTag() throws SQLException {
        statement.execute("ALTER TABLE jsons1_1 SET TAG jtag='{\"tag1\":\"femail\",\"tag2\":35,\"tag3\":true}'");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when add json tag")
    public void case03_AddTagErrorTest() throws SQLException {
        statement.execute("ALTER STABLE jsons1 add tag tag2 nchar(20)");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when delete json tag")
    public void case03_dropTagErrorTest() throws SQLException {
        statement.execute("ALTER STABLE jsons1 drop tag jtag");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when set some json tag value")
    public void case03_AlterTagErrorTest() throws SQLException {
        statement.execute("ALTER TABLE jsons1_1 SET TAG jtag=4");
    }

    @Test
    @Description("exception will throw when select syntax error")
    public void case04_SelectErrorTest() {
        int count = 0;
        for (String sql : errorSelectSql) {
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                count++;
            }
        }
        Assert.assertEquals(errorSelectSql.length, count);
    }

    @Test
    @Description("normal select stable")
    public void case04_select01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select dataint from jsons1");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(sql.length + invalidJsonInsertSql.length, count);
        close(resultSet);
    }

    @Test
    @Description("select all column from stable")
    public void case04_select02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(sql.length + invalidJsonInsertSql.length, count);
        close(resultSet);
    }

    @Test
    @Description("select json tag from stable")
    public void case04_select03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag from jsons1");
        ResultSetMetaData metaData = resultSet.getMetaData();
        metaData.getColumnTypeName(1);
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(sql.length + invalidJsonInsertSql.length + invalidJsonCreateSql.length, count);
        close(resultSet);
    }

    @Test
    @Description("where condition tag is null")
    public void case04_select04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag from jsons1 where jtag is null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(invalidJsonInsertSql.length + invalidJsonCreateSql.length, count);
        close(resultSet);
    }

    @Test
    @Description("where condition tag is not null")
    public void case04_select05() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag from jsons1 where jtag is not null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(sql.length, count);
        close(resultSet);
    }

    @Test
    @Description("select json tag")
    public void case04_select06() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag from jsons1_8");
        resultSet.next();
        String result = resultSet.getString(1);
        Assert.assertEquals("{\"tag1\":null,\"1tag$\":2,\" \":90}", result);
        close(resultSet);
    }

    @Test
    @Description("select json tag")
    public void case04_select07() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag from jsons1_1");
        resultSet.next();
        String result = resultSet.getString(1);
        Assert.assertEquals("{\"tag1\":\"femail\",\"tag2\":35,\"tag3\":true}", result);
        close(resultSet);
    }

    @Test
    @Description("select not exist json tag")
    public void case04_select08() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag from jsons1_9");
        resultSet.next();
        String result = resultSet.getString(1);
        Assert.assertNull(result);
        close(resultSet);
    }

    @Test
    @Description("select a json tag")
    public void case04_select09() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag1' from jsons1_1");
        resultSet.next();
        String result = resultSet.getString(1);
        Assert.assertEquals("\"femail\"", result);
        close(resultSet);
    }

    @Test
    @Description("select a json tag, the value is empty")
    public void case04_select10() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag2' from jsons1_6");
        resultSet.next();
        String result = resultSet.getString(1);
        Assert.assertEquals("\"\"", result);
        close(resultSet);
    }

    @Test
    @Description("select a json tag, the value is int")
    public void case04_select11() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag2' from jsons1_1");
        resultSet.next();
        String string = resultSet.getString(1);
        Assert.assertEquals("35", string);
        close(resultSet);
    }

    @Test
    @Description("select a json tag, the value is boolean")
    public void case04_select12() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag3' from jsons1_1");
        resultSet.next();
        String string = resultSet.getString(1);
        Assert.assertEquals("true", string);
        close(resultSet);
    }

    @Test
    @Description("select a json tag, the value is null")
    public void case04_select13() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag1' from jsons1_4");
        resultSet.next();
        String string = resultSet.getString(1);
        Assert.assertEquals("null", string);
        close(resultSet);
    }

    @Test
    @Description("select a json tag, the value is double")
    public void case04_select14() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag1' from jsons1_5");
        resultSet.next();
        String string = resultSet.getString(1);
        Assert.assertEquals("1.232000000", string);
        close(resultSet);
    }

    @Test
    @Description("select a json tag, the key is not exist")
    public void case04_select15() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag10' from jsons1_4");
        resultSet.next();
        String string = resultSet.getString(1);
        Assert.assertNull(string);
        close(resultSet);
    }

    @Test
    @Description("select a json tag, the result number equals tables number")
    public void case04_select16() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag1' from jsons1");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(sql.length + invalidJsonCreateSql.length + invalidJsonInsertSql.length, count);
        close(resultSet);
    }

    @Test
    @Description("where condition '=' for string")
    public void case04_select19() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2'='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("select and where conditon '=' for string")
    public void case04_select20() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select dataint,tbname,jtag->'tag1',jtag from jsons1 where jtag->'tag2'='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition result is null")
    public void case04_select21() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition equation has chinese")
    public void case04_select23() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'='收到货'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '>' for character")
    public void case05_symbolOperation01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2'>'beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '>=' for character")
    public void case05_symbolOperation02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2'>='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '<' for character")
    public void case05_symbolOperation03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2'<'beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '<=' in character")
    public void case05_symbolOperation04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2'<='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(4, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '!=' in character")
    public void case05_symbolOperation05() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2'!='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '=' empty")
    public void case05_symbolOperation06() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2'=''");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    // where json value is int
    @Test
    @Description("where condition support '=' for int")
    public void case06_selectValue01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=5");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where conditional support '<' for int")
    public void case06_selectValue02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'<54");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '<=' for int")
    public void case06_selectValue03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'<=11");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test
    @Description("where conditional support '>' for int")
    public void case06_selectValue04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'>4");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '>=' for int")
    public void case06_selectValue05() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'>=5");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where conditional support '!=' for int")
    public void case06_selectValue06() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'!=5");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where conditional support '!=' for int")
    public void case06_selectValue07() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'!=55");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test
    @Description("where conditional support '!=' for int and result is nothing")
    public void case06_selectValue08() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=10");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '=' for double")
    public void case07_selectValue01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=1.232");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '<' for double")
    public void case07_doubleOperation01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'<1.232");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '<=' for double")
    public void case07_doubleOperation02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'<=1.232");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '>' for double")
    public void case07_doubleOperation03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'>1.23");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '>=' for double")
    public void case07_doubleOperation04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'>=1.232");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '!=' for double")
    public void case07_doubleOperation05() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'!=1.232");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '!=' for double")
    public void case07_doubleOperation06() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'!=3.232");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        close(resultSet);
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when denominator is zero")
    public void case07_doubleOperation07() throws SQLException {
        statement.executeQuery("select * from jsons1 where jtag->'tag1'/0=3");
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when invalid operation")
    public void case07_doubleOperation08() throws SQLException {
        statement.executeQuery("select * from jsons1 where jtag->'tag1'/5=1");
    }

    @Test
    @Description("where condition support '=' for boolean")
    public void case08_boolOperation01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=true");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '=' for boolean")
    public void case08_boolOperation02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=false");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support '!=' for boolean")
    public void case08_boolOperation03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'!=false");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test(expected = SQLException.class)
    @Description("exception will throw when '>' operation for boolean")
    public void case08_boolOperation04() throws SQLException {
        statement.executeQuery("select * from jsons1 where jtag->'tag1'>false");
    }

    @Test
    @Description("where conditional support '=null'")
    public void case09_select01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where conditional support 'is null'")
    public void case09_select02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag is null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support 'is not null'")
    public void case09_select03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag is not null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(8, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support one tag '='")
    public void case09_select04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag_no_exist'=3");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support one tag 'is null'")
    public void case09_select05() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1' is null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(invalidJsonInsertSql.length, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support one tag 'is null'")
    public void case09_select06() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag4' is null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(sql.length + invalidJsonInsertSql.length, count);
        close(resultSet);
    }

    @Test
    @Description("where condition support one tag 'is not null'")
    public void case09_select07() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag3' is not null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(4, count);
        close(resultSet);
    }

    @Test
    @Description("contains")
    public void case09_select10() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag contains 'tag1'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(8, count);
        close(resultSet);
    }

    @Test
    @Description("contains")
    public void case09_select11() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag contains 'tag3'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(4, count);
        close(resultSet);
    }

    @Test
    @Description("contains with no exist tag")
    public void case09_select12() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag contains 'tag_no_exist'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition with and")
    public void case10_selectAndOr01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition with 'or'")
    public void case10_selectAndOr02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=false or jtag->'tag2'='beijing'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition with 'and'")
    public void case10_selectAndOr03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition with 'or'")
    public void case10_selectAndOr04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition with 'or' and contains")
    public void case10_selectAndOr05() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1' is not null and jtag contains 'tag3'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(4, count);
        close(resultSet);
    }

    @Test
    @Description("where condition with 'and' and contains")
    public void case10_selectAndOr06() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1'='femail' and jtag contains 'tag3'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("test with tbname/normal column")
    public void case11_selectTbName01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where tbname = 'jsons1_1'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("test with tbname/normal column")
    public void case11_selectTbName02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("test with tbname/normal column")
    public void case11_selectTbName03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=3");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("test with tbname/normal column")
    public void case11_selectTbName04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=23");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("where condition like")
    public void case12_selectWhere01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select *,tbname from jsons1 where jtag->'tag2' like 'bei%'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition like")
    public void case12_selectWhere02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select *,tbname from jsons1 where jtag->'tag1' like 'fe%' and jtag->'tag2' is not null");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test(expected = SQLException.class)
    @Description("where condition in no support in")
    public void case12_selectWhere03() throws SQLException {
        statement.executeQuery("select * from jsons1 where jtag->'tag1' in ('beijing')");
    }

    @Test
    @Description("where condition match")
    public void case12_selectWhere04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1' match 'ma'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition match")
    public void case12_selectWhere05() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1' match 'ma$'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("where condition match")
    public void case12_selectWhere06() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag2' match 'jing$'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        close(resultSet);
    }

    @Test
    @Description("where condition match")
    public void case12_selectWhere07() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from jsons1 where jtag->'tag1' match '收到'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("insert distinct")
    public void case13_selectDistinct01() throws SQLException {
        statement.execute("insert into jsons1_14 using jsons1 tags('{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '你就会', 'dws')");
    }

    @Test
    @Description("distinct json tag")
    public void case13_selectDistinct02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select distinct jtag->'tag1' from jsons1");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(8, count);
        close(resultSet);
    }

    @Test
    @Description("distinct json tag")
    public void case13_selectDistinct03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select distinct jtag from jsons1");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(9, count);
        close(resultSet);
    }

    @Test
    @Description("insert json tag")
    public void case14_selectDump01() throws SQLException {
        statement.execute("INSERT INTO jsons1_15 using jsons1 tags('{\"tbname\":\"tt\",\"databool\":true,\"datastr\":\"是是是\"}') values(1591060828000, 4, false, 'jjsf', \"你就会\")");
    }

    @Test
    @Description("test duplicate key with normal column")
    public void case14_selectDump02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select *,tbname,jtag from jsons1 where jtag->'datastr' match '是' and datastr match 'js'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        close(resultSet);
    }

    @Test
    @Description("test duplicate key with normal column")
    public void case14_selectDump03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select tbname,jtag->'tbname' from jsons1 where jtag->'tbname'='tt' and tbname='jsons1_14'");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(0, count);
        close(resultSet);
    }

    @Test
    @Description("insert json tag for join test")
    public void case15_selectJoin01() throws SQLException {
        statement.execute("create table if not exists jsons2(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)");
        statement.execute("insert into jsons2_1 using jsons2 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 2, false, 'json2', '你是2')");
        statement.execute("insert into jsons2_2 using jsons2 tags('{\"tag1\":5,\"tag2\":null}') values (1591060628000, 2, true, 'json2', 'sss')");

        statement.execute("create table if not exists jsons3(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)");
        statement.execute("insert into jsons3_1 using jsons3 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 3, false, 'json3', '你是3')");
        statement.execute("insert into jsons3_2 using jsons3 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060638000, 2, true, 'json3', 'sss')");
    }

    @Test
    @Description("select json tag from join")
    public void case15_selectJoin02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select 'sss',33,a.jtag->'tag3' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'");
        resultSet.next();
        Assert.assertEquals("sss", resultSet.getString(1));
        close(resultSet);
    }

    @Test
    @Description("group by and order by json tag desc")
    public void case16_selectGroupOrder01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' desc");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(8, count);
        close(resultSet);
    }

    @Test
    @Description("group by and order by json tag asc")
    public void case16_selectGroupOrder02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' asc");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(8, count);
        close(resultSet);
    }

    @Test
    @Description("stddev with group by json tag")
    public void case17_selectStddev01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select stddev(dataint) from jsons1 group by jtag->'tag1'");
        String s = "";
        int count = 0;
        while (resultSet.next()) {
            count++;
            s = resultSet.getString(2);

        }
        Assert.assertEquals(8, count);
        Assert.assertEquals("\"收到货\"", s);
        close(resultSet);
    }

    @Test
    @Description("subquery json tag")
    public void case18_selectSubquery01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select * from (select jtag, dataint from jsons1)");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(11, count);
        close(resultSet);
    }

    @Test
    @Description("subquery some json tags")
    public void case18_selectSubquery02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag1' from (select jtag->'tag1', dataint from jsons1)");

        ResultSetMetaData metaData = resultSet.getMetaData();
        String columnName = metaData.getColumnName(1);
        Assert.assertEquals("jtag->'tag1'", columnName);

        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(11, count);
        close(resultSet);
    }

    @Test
    @Description("query some json tags from subquery")
    public void case18_selectSubquery04() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select ts,tbname,jtag->'tag1' from (select jtag->'tag1',tbname,ts from jsons1 order by ts)");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        Assert.assertEquals(11, count);
        close(resultSet);
    }

    @Test
    @Description("query metadata for json")
    public void case19_selectMetadata01() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag from jsons1");
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnType = metaData.getColumnType(1);
        String columnTypeName = metaData.getColumnTypeName(1);
        Assert.assertEquals(Types.OTHER, columnType);
        Assert.assertEquals("JSON", columnTypeName);
        close(resultSet);
    }

    @Test
    @Description("query metadata for json")
    public void case19_selectMetadata02() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select *,jtag from jsons1");
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnType = metaData.getColumnType(6);
        String columnTypeName = metaData.getColumnTypeName(6);
        Assert.assertEquals(Types.OTHER, columnType);
        Assert.assertEquals("JSON", columnTypeName);
        close(resultSet);
    }

    @Test
    @Description("query metadata for one json result")
    public void case19_selectMetadata03() throws SQLException {
        ResultSet resultSet = statement.executeQuery("select jtag->'tag1' from jsons1_6");
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnType = metaData.getColumnType(1);
        String columnTypeName = metaData.getColumnTypeName(1);
        Assert.assertEquals(Types.OTHER, columnType);
        Assert.assertEquals("JSON", columnTypeName);
        resultSet.next();
        String string = resultSet.getString(1);
        Assert.assertEquals("11", string);
        close(resultSet);
    }

    @Test
    @Description("stmt batch insert with json tag")
    public void case20_batchInsert() throws SQLException {
        String jsonTag = "{\"tag1\":\"fff\",\"tag2\":5,\"tag3\":true}";
        statement.execute("drop table if exists jsons5");
        statement.execute("CREATE STABLE IF NOT EXISTS jsons5 (ts timestamp, dataInt int, dataStr nchar(20)) TAGS(jtag json)");

        String sql = "INSERT INTO ? USING jsons5 TAGS (?) VALUES ( ?,?,? )";

        try (PreparedStatement pst = connection.prepareStatement(sql)) {
            TSDBPreparedStatement ps = pst.unwrap(TSDBPreparedStatement.class);
            // 设定数据表名：
            ps.setTableName("batch_test");
            // 设定 TAGS 取值 setTagNString or setTagJson：
//            ps.setTagNString(0, jsonTag);
            ps.setTagJson(0, jsonTag);

            // VALUES 部分以逐列的方式进行设置：
            int numOfRows = 4;
            ArrayList<Long> ts = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                ts.add(System.currentTimeMillis() + i);
            }
            ps.setTimestamp(0, ts);

            Random r = new Random();
            int random = 10 + r.nextInt(5);
            ArrayList<Integer> c1 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    c1.add(null);
                } else {
                    c1.add(r.nextInt());
                }
            }
            ps.setInt(1, c1);

            ArrayList<String> c2 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                c2.add("分支" + i % 4);
            }
            ps.setNString(2, c2, 10);

            // AddBatch 之后，缓存并未清空。为避免混乱，并不推荐在 ExecuteBatch 之前再次绑定新一批的数据：
            ps.columnDataAddBatch();
            // 执行绑定数据后的语句：
            ps.columnDataExecuteBatch();
        }

        ResultSet resultSet = statement.executeQuery("select jtag from batch_test");
        ResultSetMetaData metaData = resultSet.getMetaData();
        String columnName = metaData.getColumnName(1);
        Assert.assertEquals("jtag", columnName);
        Assert.assertEquals("JSON", metaData.getColumnTypeName(1));
        resultSet.next();
        String string = resultSet.getString(1);
        Assert.assertEquals(jsonTag, string);
        resultSet.close();
        resultSet = statement.executeQuery("select jtag->'tag2' from batch_test");
        resultSet.next();
        long l = resultSet.getLong(1);
        Assert.assertEquals(5, l);
        resultSet.close();
    }

    private void close(ResultSet resultSet) {
        try {
            if (null != resultSet) {
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() {
        String host = "127.0.0.1";
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();
            statement.execute("drop database if exists " + dbName);
            statement.execute("create database if not exists " + dbName);
            statement.execute("use " + dbName);
            statement.execute(superSql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (null != statement) {
                statement.execute("drop database " + dbName);
                statement.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
