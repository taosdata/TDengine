package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.rs.RestfulDatabaseMetaData;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "sheyj", version = "2.3.7")
@FixMethodOrder
public class WSQueryBIModeTest {
    private final String db_name = TestUtils.camelToSnake(WSQueryBIModeTest.class);
    private Connection connection;

    @Description("query")
    @Ignore
    @Test
    public void queryTbname()  {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM ws_bi_mode.meters LIMIT 1");
            resultSet.next();
            Assert.assertEquals(7, resultSet.getMetaData().getColumnCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Description("query view")
    @Ignore
    @Test
    public void queryView()  {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM ws_bi_mode.view_test LIMIT 1");
            resultSet.next();
            Assert.assertEquals(1, resultSet.getRow());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Description("get column view")
    @Ignore
    @Test
    public void getColumnView() throws SQLException {
        RestfulDatabaseMetaData metaData = connection.getMetaData().unwrap(RestfulDatabaseMetaData.class);
        ResultSet columns = metaData.getColumns("ws_bi_mode", "", "view_test", null);
        // then
        ResultSetMetaData meta = columns.getMetaData();
        columns.next();
        // column: 1
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertEquals(db_name, columns.getString(1));
            Assert.assertEquals(db_name, columns.getString("TABLE_CAT"));
            // TABLE_NAME
            Assert.assertEquals("TABLE_NAME", meta.getColumnLabel(3));
            Assert.assertEquals("view_test", columns.getString(3));
            Assert.assertEquals("view_test", columns.getString("TABLE_NAME"));
            // COLUMN_NAME
            Assert.assertEquals("COLUMN_NAME", meta.getColumnLabel(4));
            Assert.assertEquals("ts", columns.getString(4));
            Assert.assertEquals("ts", columns.getString("COLUMN_NAME"));
            // DATA_TYPE
            Assert.assertEquals("DATA_TYPE", meta.getColumnLabel(5));
            Assert.assertEquals(Types.TIMESTAMP, columns.getInt(5));
            Assert.assertEquals(Types.TIMESTAMP, columns.getInt("DATA_TYPE"));
            // TYPE_NAME
            Assert.assertEquals("TYPE_NAME", meta.getColumnLabel(6));
            Assert.assertEquals("TIMESTAMP", columns.getString(6));
            Assert.assertEquals("TIMESTAMP", columns.getString("TYPE_NAME"));
            // COLUMN_SIZE
            Assert.assertEquals("COLUMN_SIZE", meta.getColumnLabel(7));
            Assert.assertEquals(23, columns.getInt(7));
            Assert.assertEquals(23, columns.getInt("COLUMN_SIZE"));
            // DECIMAL_DIGITS
            Assert.assertEquals("DECIMAL_DIGITS", meta.getColumnLabel(9));
            Assert.assertEquals(0, columns.getInt(9));
            Assert.assertEquals(0, columns.getInt("DECIMAL_DIGITS"));
            Assert.assertEquals(null, columns.getString(9));
            Assert.assertEquals(null, columns.getString("DECIMAL_DIGITS"));
            // NUM_PREC_RADIX
            Assert.assertEquals("NUM_PREC_RADIX", meta.getColumnLabel(10));
            Assert.assertEquals(10, columns.getInt(10));
            Assert.assertEquals(10, columns.getInt("NUM_PREC_RADIX"));
            // NULLABLE
            Assert.assertEquals("NULLABLE", meta.getColumnLabel(11));
            Assert.assertEquals(DatabaseMetaData.columnNoNulls, columns.getInt(11));
            Assert.assertEquals(DatabaseMetaData.columnNoNulls, columns.getInt("NULLABLE"));
            // REMARKS
            Assert.assertEquals("REMARKS", meta.getColumnLabel(12));
            Assert.assertEquals("VIEW COL", columns.getString(12));
            Assert.assertEquals("VIEW COL", columns.getString("REMARKS"));
        }
        columns.next();
        // column: 2
        {
            // TABLE_CAT
            Assert.assertEquals("TABLE_CAT", meta.getColumnLabel(1));
            Assert.assertEquals(db_name, columns.getString(1));
            Assert.assertEquals(db_name, columns.getString("TABLE_CAT"));
            // TABLE_NAME
            Assert.assertEquals("TABLE_NAME", meta.getColumnLabel(3));
            Assert.assertEquals("view_test", columns.getString(3));
            Assert.assertEquals("view_test", columns.getString("TABLE_NAME"));
            // COLUMN_NAME
            Assert.assertEquals("COLUMN_NAME", meta.getColumnLabel(4));
            Assert.assertEquals("location", columns.getString(4));
            Assert.assertEquals("location", columns.getString("COLUMN_NAME"));
            // DATA_TYPE
            Assert.assertEquals("DATA_TYPE", meta.getColumnLabel(5));
            Assert.assertEquals(Types.VARCHAR, columns.getInt(5));
            Assert.assertEquals(Types.VARCHAR, columns.getInt("DATA_TYPE"));
            // TYPE_NAME
            Assert.assertEquals("TYPE_NAME", meta.getColumnLabel(6));
            Assert.assertEquals("VARCHAR", columns.getString(6));
            Assert.assertEquals("VARCHAR", columns.getString("TYPE_NAME"));
            // COLUMN_SIZE
            Assert.assertEquals("COLUMN_SIZE", meta.getColumnLabel(7));
            Assert.assertEquals(64, columns.getInt(7));
            Assert.assertEquals(64, columns.getInt("COLUMN_SIZE"));
            // DECIMAL_DIGITS
            Assert.assertEquals("DECIMAL_DIGITS", meta.getColumnLabel(9));
            Assert.assertEquals(0, columns.getInt(9));
            Assert.assertEquals(0, columns.getInt("DECIMAL_DIGITS"));
            // NUM_PREC_RADIX
            Assert.assertEquals("NUM_PREC_RADIX", meta.getColumnLabel(10));
            Assert.assertEquals(10, columns.getInt(10));
            Assert.assertEquals(10, columns.getInt("NUM_PREC_RADIX"));
            // NULLABLE
            Assert.assertEquals("NULLABLE", meta.getColumnLabel(11));
            Assert.assertEquals(DatabaseMetaData.columnNullable, columns.getInt(11));
            Assert.assertEquals(DatabaseMetaData.columnNullable, columns.getInt("NULLABLE"));
            // REMARKS
            Assert.assertEquals("REMARKS", meta.getColumnLabel(12));
            Assert.assertEquals("VIEW COL", columns.getString(12));
        }
    }

    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS-RS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true&conmode=1";
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();

        statement.executeQuery("drop database if exists ws_bi_mode");
        statement.executeQuery("create database ws_bi_mode keep 36500");
        statement.executeQuery("use ws_bi_mode");

        statement.executeQuery("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)");
        statement.executeQuery("CREATE TABLE d1001 USING meters TAGS ('California.SanFrancisco', 2)");
        statement.executeQuery("INSERT INTO d1001 USING meters TAGS ('California.SanFrancisco', 2) VALUES (NOW, 10.2, 219, 0.32)");

        statement.executeQuery("create view view_test as (select ts, location, phase from meters)");

        statement.close();

    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }
}

