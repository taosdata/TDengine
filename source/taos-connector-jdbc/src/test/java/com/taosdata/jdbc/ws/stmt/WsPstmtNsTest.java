package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.sql.*;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import static com.taosdata.jdbc.TSDBConstants.TIMESTAMP_DATA_OUT_OF_RANGE;

@FixMethodOrder
public class WsPstmtNsTest {
            final String db_name = TestUtils.camelToSnake(WsPstmtNsTest.class);
    final String tableName = "wpt";
    String superTable = "wpt_st";
    Connection connection;

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 1);
        int i = statement.executeUpdate();
        Assert.assertEquals(1, i);
        statement.close();
    }

    @Test
    @Ignore
    public void testChangePrecision() throws SQLException,InterruptedException {

        // init pstmt, and insert one row successfully
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 1);
        int i = statement.executeUpdate();
        Assert.assertEquals(1, i);

        // 2. change precision to 'ms'
        initDbAndTable("ms");

        // 3. insert data failed
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 1);
        try {
            statement.executeUpdate();
        } catch (SQLException e) {
            Assert.assertEquals(TIMESTAMP_DATA_OUT_OF_RANGE, e.getErrorCode());
        }

        // 3. retry to insert data success
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 1);
        i = statement.executeUpdate();
        Assert.assertEquals(1, i);

        // 4. change precision to 'ns'
        initDbAndTable("ns");
        statement.close();
    }
    @Test
    public void testExecuteBatchInsert() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            statement.setTimestamp(1, new Timestamp(System.currentTimeMillis() + i));
            statement.setInt(2, i);
            statement.addBatch();
        }
        statement.executeBatch();

        String sql1 = "select * from " + db_name + "." + tableName;
        statement = connection.prepareStatement(sql1);
        boolean b = statement.execute();
        Assert.assertTrue(b);
        ResultSet resultSet = statement.getResultSet();
        HashSet<Object> collect = Arrays.stream(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
                .collect(HashSet::new, HashSet::add, AbstractCollection::addAll);
        while (resultSet.next()) {
            Assert.assertTrue(collect.contains(resultSet.getInt(2)));
        }
        statement.close();
    }

    @Test
    public void testQuery() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            statement.setTimestamp(1, new Timestamp(System.currentTimeMillis() + i));
            statement.setInt(2, i);
            statement.addBatch();
        }
        statement.executeBatch();
        statement.close();

        sql = "select * from " + db_name + "." + tableName + " where ts > ? and ts < ?";
        statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setTimestamp(2, new Timestamp(System.currentTimeMillis() + 1000));
        ResultSet resultSet = statement.executeQuery();

        int i = 0;
        while (resultSet.next()) {
            Assert.assertEquals(resultSet.getInt(2), i++);
        }
    }

    private void initDbAndTable(String precision) throws SQLException, InterruptedException{
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        TestUtils.waitTransactionDone(connection);
        statement.execute("create database " + db_name + " PRECISION '" + precision + "'");
        TestUtils.waitTransactionDone(connection);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + tableName + " (ts timestamp, c1 int)");

        statement.close();
    }

    @Before
    public void before() throws SQLException, InterruptedException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        initDbAndTable("ns");
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
            TestUtils.waitTransactionDone(connection);

        } catch (Exception e) {
            throw new RuntimeException(e);
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