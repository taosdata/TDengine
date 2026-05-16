package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class AbstractDatabaseMetaDataColumnTest {
    static Connection connection;
    static final String HOST = TestEnvUtil.getHost();
    static DatabaseMetaData metaData;
    static final String DB_NAME = TestUtils.camelToSnake(AbstractDatabaseMetaDataColumnTest.class);

    @Test
    public void getColumnsAllNull() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, null, null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertTrue(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsAll() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, "ins_tables", "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        int count = 0;
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
            count++;
        }
        Assert.assertEquals(1, count);
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsAll2() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, null, null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        int count = 0;
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
            count++;
        }
        Assert.assertTrue(count > 1);
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }
    @Test
    public void getColumns3() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, null, "table_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        int count = 0;
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
            count++;
        }
        Assert.assertTrue(count > 1);
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsCatalog() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, null, null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));

        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsTable() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, "ins_tables", null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        int count = 0;
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
            count++;
        }
        Assert.assertEquals(10, count);
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsColumn() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, null, "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        int count = 0;
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
            count++;
        }
        Assert.assertTrue(count >= 3);
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsCatalogTable() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, "ins_tables", null);
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        int count = 0;
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
            count++;
        }
        Assert.assertEquals(10, count);
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsCatalogColumn() throws SQLException {
        ResultSet columns = metaData.getColumns("information_schema", null, null, "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsTableColumn() throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, "ins_tables", "stable_name");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        int count = 0;
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
            count++;
        }
        Assert.assertEquals(1, count);
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertFalse(dbs.contains("performance_schema"));
    }

    @Test
    public void getColumnsPercent() throws SQLException {
        ResultSet columns = metaData.getColumns("%", null, "%", "%");
        Set<String> dbs = new HashSet<>();
        Set<String> tables = new HashSet<>();
        while (columns.next()) {
            dbs.add(columns.getString("TABLE_CAT"));
            tables.add(columns.getString("TABLE_NAME"));
        }
        Assert.assertTrue(dbs.contains("information_schema"));
        Assert.assertTrue(tables.contains("ins_tables"));
        Assert.assertTrue(dbs.contains("performance_schema"));
    }

    @BeforeClass
    public static void before() throws SQLException, InterruptedException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url);
        metaData = connection.getMetaData();
        TestUtils.waitTransactionDone(connection);

        try(Statement stmt = connection.createStatement()){
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME);
            stmt.execute("use " + DB_NAME);
            stmt.execute("CREATE STABLE meters(ts timestamp,current float,voltage int,phase float) TAGS (location varchar(64),group_id int);");
            stmt.execute("INSERT INTO d1001 USING meters TAGS (\"California.SanFrancisco\", 2) VALUES \n" +
                    "    (\"2018-10-03 14:38:05\", 10.2, 220, 0.23),\n" +
                    "    (\"2018-10-03 14:38:15\", 12.6, 218, 0.33),\n" +
                    "    (\"2018-10-03 14:38:25\", 12.3, 221, 0.31) \n");
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        if (connection != null) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("drop database if exists " + DB_NAME);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            connection.close();
        }
    }
}