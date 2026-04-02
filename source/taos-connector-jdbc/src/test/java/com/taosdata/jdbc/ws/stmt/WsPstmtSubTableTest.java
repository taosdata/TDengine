package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.math.BigInteger;
import java.sql.*;
import java.util.Properties;

public class WsPstmtSubTableTest {
            final String db_name = TestUtils.camelToSnake(WsPstmtSubTableTest.class);
    final String superTable = "wpt_st";
    final String superTable1 = "wpt_st1";
    final String superTable2 = "wpt_json";
    Connection connection;

    @Test
    public void testInsertSubTable() throws SQLException {
        String sql = "insert into ? using " + db_name + "." + superTable + " tags (?) values(?, ?)";
        TSWSPreparedStatement statement = (TSWSPreparedStatement) connection.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            long current = System.currentTimeMillis();
            statement.setTableName(db_name + ".t" + i);
            statement.setTagInt(0, i);
            for (int j = 0; j < 10; j++) {
                statement.setTimestamp(1, new Timestamp(current + j));
                statement.setInt(2, j * 10);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        ResultSet resultSet = statement.executeQuery("show " + db_name + ".tables");
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        resultSet.close();
        statement.close();
        Assert.assertEquals(10, count);

        statement = (TSWSPreparedStatement) connection.prepareStatement(
                "insert into ? using " + db_name + "." + superTable1 +
                        " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  values (?, ?)");
        statement.setTableName(db_name + ".t_with_tag");
        statement.setTagByte(0, (byte) 1);
        statement.setTagShort(1, (short) 2);
        statement.setTagInt(2, 3);
        statement.setTagLong(3, 4L);
        statement.setTagFloat(4, 5.0f);
        statement.setTagDouble(5, 6.0);
        statement.setTagString(6, "涛思");
        statement.setTagNString(7, "数据");
        statement.setTagBoolean(8, true);
        statement.setTagTimestamp(9, new Timestamp(System.currentTimeMillis()));
        statement.setTagString(10, "taosdata");
        statement.setTagShort(11, TSDBConstants.MAX_UNSIGNED_BYTE);
        statement.setTagInt(12,  TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setTagLong(13,  TSDBConstants.MAX_UNSIGNED_INT);
        statement.setTagBigInteger(14, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 100);

        statement.executeUpdate();
        ResultSet resultSet1 = statement.executeQuery("select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15 from " + db_name + ".t_with_tag");
        resultSet1.next();
        Assert.assertEquals(1, resultSet1.getByte(1));
        Assert.assertEquals(2, resultSet1.getShort(2));
        Assert.assertEquals(3, resultSet1.getInt(3));
        Assert.assertEquals(4L, resultSet1.getLong(4));
        Assert.assertEquals(5.0f, resultSet1.getFloat(5), 0.0001);
        Assert.assertEquals(6.0, resultSet1.getDouble(6), 0.0001);
        Assert.assertEquals("涛思", resultSet1.getString(7));
        Assert.assertEquals("数据", resultSet1.getNString(8));
        Assert.assertTrue(resultSet1.getBoolean(9));
        Assert.assertEquals("taosdata", resultSet1.getString(11));

        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet1.getShort(12));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet1.getInt(13));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet1.getLong(14));
        Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet1.getObject(15));

        resultSet1.close();
        statement.close();

        statement = (TSWSPreparedStatement) connection.prepareStatement(
                "insert into ? using " + db_name + "." + superTable2 +
                        " tags (?)  values (?, ?)");
        statement.setTableName(db_name + ".t_with_json");
        String tagJson = "{\n" +
                "                    \"t1\": 1,\n" +
                "                    \"t2\": 2,\n" +
                "                    \"t3\": 3,\n" +
                "                    \"t4\": 4,\n" +
                "                    \"t5\": 5.0,\n" +
                "                    \"t6\": 6.0,\n" +
                "                    \"t7\": \"涛思\",\n" +
                "                    \"t8\": \"数据\",\n" +
                "                    \"t9\": true,\n" +
                "                    \"t10\": \"2021-01-01 00:00:00.000\",\n" +
                "                    \"t11\": \"taosdata\"\n" +
                "                }"; // json string
        statement.setTagJson(0, tagJson);
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 100);
        statement.executeUpdate();
        ResultSet resultSet2 = statement.executeQuery("select tt->'t1' from " + db_name + ".t_with_json");
        resultSet2.next();
        Assert.assertEquals(1.0, resultSet2.getDouble(1), 0.0001);
        resultSet2.close();
        statement.close();

    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + superTable + " (ts timestamp, c1 int) tags(t1 int)");
        statement.execute("create table if not exists " + db_name + "." + superTable1 + " (ts timestamp, c1 int) " +
                "tags(t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 binary(10), t8 nchar(10), " +
                "t9 bool, t10 timestamp, t11 varchar(10), t12 tinyint unsigned, t13 smallint unsigned, t14 int unsigned, t15 bigint unsigned)");
        statement.execute("create table if not exists " + db_name + "." + superTable2 + " (ts timestamp, c1 int) tags (tt json)");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
    }

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}

