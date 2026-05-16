package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.sql.*;
import java.util.*;

@FixMethodOrder
public class WsPstmtStmt2Test {
            final String db_name = TestUtils.camelToSnake(WsPstmtStmt2Test.class);
    final String tableName = "wpt";
    String superTable = "wpt_st";
    Connection connection;

    final int numOfSubTable = 10;
    final int numOfRow = 10;
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void testStmt2Insert() throws SQLException {
        String sql = "INSERT INTO ? USING " + db_name + "." + tableName + " TAGS(?,?) VALUES (?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                //set table name
                pstmt.setTableName("d_bind_" + i);

                // set tags
                pstmt.setTagInt(0, i);
                pstmt.setTagString(1, "location_" + i);

                // set column ts
                ArrayList<Long> tsList = new ArrayList<>();
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++)
                    tsList.add(current + j);
                pstmt.setTimestamp(0, tsList);

                // set column current
                ArrayList<Float> currentList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    currentList.add(random.nextFloat() * 30);
                pstmt.setFloat(1, currentList);

                // set column voltage
                ArrayList<Integer> voltageList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    voltageList.add(random.nextInt(300));
                pstmt.setInt(2, voltageList);

                // set column phase
                ArrayList<Float> phaseList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    phaseList.add(random.nextFloat());
                pstmt.setFloat(3, phaseList);
                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
            // you can check exeResult here
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
        }
    }

    @Test
    public void testStmt2InsertExtend() throws SQLException {
        createSubTables();
        String sql = "INSERT INTO " + db_name + "." + tableName + " (tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                //set table name
                pstmt.setTableName("d_bind_" + i);

                // set column ts
                ArrayList<Long> tsList = new ArrayList<>();
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++)
                    tsList.add(current + j);
                pstmt.setTimestamp(0, tsList);

                // set column current
                ArrayList<Float> currentList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    currentList.add(random.nextFloat() * 30);
                pstmt.setFloat(1, currentList);

                // set column voltage
                ArrayList<Integer> voltageList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    voltageList.add(random.nextInt(300));
                pstmt.setInt(2, voltageList);

                // set column phase
                ArrayList<Float> phaseList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    phaseList.add(random.nextFloat());
                pstmt.setFloat(3, phaseList);
                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
            // you can check exeResult here
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
        }
    }

    @Test
    public void testStmt2InsertMixedApi() throws SQLException {
        String sql = "INSERT INTO ? USING " + db_name + "." + tableName + " TAGS(?,?) VALUES (?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                //set table name
                pstmt.setTableName("d_bind_" + i);

                // set tags
                pstmt.setTagInt(0, i);
                pstmt.setTagString(1, "location_" + i);

                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setTimestamp(1, new Timestamp(current + j));
                    pstmt.setFloat(2, random.nextFloat() * 30);
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setFloat(4, random.nextFloat());
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            // you can check exeResult here
            System.out.println("Successfully inserted " + (numOfSubTable * numOfRow) + " rows to power.meters.");
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
        }
    }

    @Test
    public void testStmt2InsertSubTable() throws SQLException {
        String sql = "INSERT INTO stb1 USING " + db_name + "." + tableName + " TAGS(?,?) VALUES (?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            long current = System.currentTimeMillis();
            for (int i = 1; i <= 10; i++) {
                // set tags
                pstmt.setTagInt(0, 1);
                pstmt.setTagString(1, "location_" + 1);

                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setTimestamp(1, new Timestamp(current++));
                    pstmt.setFloat(2, random.nextFloat() * 30);
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setFloat(4, random.nextFloat());
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            // you can check exeResult here
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
        }
    }

    @Test
    public void testStmt2InsertStdApi() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, groupId, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setString(1, "d_bind_中国人" + i);
                    pstmt.setInt(2, i);
                    pstmt.setString(3, "location_" + i);

                    pstmt.setTimestamp(4, new Timestamp(current + j));
                    pstmt.setFloat(5, random.nextFloat() * 30);
                    pstmt.setInt(6, random.nextInt(300));
                    pstmt.setFloat(7, random.nextFloat());

                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
            Assert.assertEquals((numOfRow), Utils.getSqlRows(connection, db_name + "." + "`d_bind_中国人1`"));
        }
    }

    @Test
    public void testStmt2InsertStdApi2() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, groupId, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            for (int i = 1; i <= 100; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < 1; j++) {
                    pstmt.setString(1, "d_bind_中国人" + i);
                    pstmt.setInt(2, i);
                    pstmt.setString(3, "location_" + i);

//                    pstmt.setTimestamp(4, new Timestamp(current + j));
//                    pstmt.setFloat(5, random.nextFloat() * 30);
//                    pstmt.setInt(6, random.nextInt(300));
//                    pstmt.setFloat(7, random.nextFloat());

                    pstmt.setTimestamp(4, new Timestamp(1746870173341L));
                    pstmt.setFloat(5, 1.0f);
                    pstmt.setInt(6, 2);
                    pstmt.setFloat(7, 3.0f);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            Assert.assertEquals((100), Utils.getSqlRows(connection, db_name + "." + tableName));
            Assert.assertEquals((1), Utils.getSqlRows(connection, db_name + "." + "`d_bind_中国人1`"));
        }
    }

    @Test
    public void testStmt2InsertStdApiTableExist() throws SQLException {
        // make sure sub table exists
        try (Statement stmt = connection.createStatement()) {
            for (int i = 1; i <= numOfSubTable; i++) {
                stmt.execute("create table if not exists d_bind_" + i + " using " + db_name + "." + tableName + " tags(" + i + ", \"location_" + i + "\")");
            }
        } catch (Exception ex) {
            System.out.printf("Failed to create sub table, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            ex.printStackTrace();
            throw ex;
        }

        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setString(1, "d_bind_" + i);

                    pstmt.setTimestamp(2, new Timestamp(current + j));
                    pstmt.setFloat(3, random.nextFloat() * 30);
                    pstmt.setInt(4, random.nextInt(300));
                    pstmt.setFloat(5, random.nextFloat());
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            // you can check exeResult here
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
        }
    }

    @Test
    public void testStmt2InsertStdApiWithEscapeChar() throws SQLException {
        String sql = "INSERT INTO `" + db_name + "`.`" + tableName + "` (tbname, groupId, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setString(1, "d_bind_" + i);
                    pstmt.setInt(2, i);
                    pstmt.setString(3, "location_" + i);

                    pstmt.setTimestamp(4, new Timestamp(current + j));
                    pstmt.setFloat(5, random.nextFloat() * 30);
                    pstmt.setInt(6, random.nextInt(300));
                    pstmt.setFloat(7, random.nextFloat());
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
        }
    }

    private void createSubTables() throws SQLException {
        String createSql = "create table";
        for (int i = 1; i <= numOfSubTable; i++) {
            createSql += " if not exists d_bind_" + i + " using " + db_name + "." + tableName + " tags(" + i + ", \"location_" + i + "\")";
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSql);
        }
    }

    @Test
    public void testStmt2InsertStdApiNoTag() throws SQLException {
        // create sub table first
        createSubTables();

        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setString(1, "d_bind_" + i);

                    pstmt.setTimestamp(2, new Timestamp(current + j));
                    pstmt.setFloat(3, random.nextFloat() * 30);
                    pstmt.setInt(4, random.nextInt(300));
                    pstmt.setFloat(5, random.nextFloat());
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            Assert.assertEquals((numOfSubTable * numOfRow), Utils.getSqlRows(connection, db_name + "." + tableName));
        }

        String sql2 = "select * from " + db_name + "." + tableName + " where ts > ? and ts < ? order by ts desc limit ?, ?";

        try (TSWSPreparedStatement pstmt = connection.prepareStatement(sql2).unwrap(TSWSPreparedStatement.class)) {
            pstmt.setTimestamp(0, new Timestamp(System.currentTimeMillis() - 1000));
            pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmt.setInt(2, 5);
            pstmt.setInt(3, 6);
            ResultSet rs = pstmt.executeQuery();

            int rows = 0;
            while (rs.next()) {
                rows++;
                System.out.println("ts-1: " + rs.getTimestamp(1) + ", current: " + rs.getFloat(2) + ", voltage: " + rs.getInt(3) + ", phase: " + rs.getFloat(4));
            }
            Assert.assertEquals(6, rows);
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create stable if not exists " + db_name + "." + tableName + " (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
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

