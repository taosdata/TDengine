package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.*;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WsPstmtTest {
            final String db_name = TestUtils.camelToSnake(WsPstmtTest.class);
    final String tableName = "wpt";
    String superTable = "wpt_st";
    Connection connection;

    PreparedStatement pstmt;

    @Test
    public void test001_ExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(1746870173341L));
        statement.setInt(2, 1);
        int i = statement.executeUpdate();
        Assert.assertEquals(1, i);
        statement.close();
    }

    @Test
    public void test002_ReuseStmtExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 1);
        int i = statement.executeUpdate();
        Assert.assertEquals(1, i);

        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        statement.setInt(2, 1);
        i = statement.executeUpdate();
        Assert.assertEquals(1, i);
        statement.close();
    }

    @Test
    public void test003_ExecuteBatchInsert() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " (ts, c1) values(?, ?)";
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
    public void test004_Query() throws SQLException {
        String sql = "select * from " + db_name + "." + tableName + " where ts > ? and ts < ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis() - 1000));
        statement.setTimestamp(2, new Timestamp(System.currentTimeMillis() + 1000));
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getTimestamp(1) + " " + resultSet.getInt(2));
        }
    }
    @Test
    public void test005_NormalQuery() throws SQLException {
        pstmt.execute("insert into " + db_name + "." + tableName + " values  (now, 1)");

        String sql = "select * from " + db_name + "." + tableName + " limit 1";
        try (PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery()){
            Assert.assertTrue(resultSet.next());
        }
    }

    @Test(expected = SQLException.class)
    public void test006_QuerySyntaxError() throws SQLException {
        String sql = "select * from " + db_name + "." + tableName + " wheree ts > ? and ts < ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(System.currentTimeMillis() - 1000));
        statement.setTimestamp(2, new Timestamp(System.currentTimeMillis() + 1000));
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getTimestamp(1) + " " + resultSet.getInt(2));
        }
    }

    @Test (expected = SQLException.class)
    public void test100_SetNCharacterStream() throws SQLException {
        pstmt.setNCharacterStream(1, null);
    }

    @Test (expected = SQLException.class)
    public void test101_SetNCharacterStream2() throws SQLException {
        pstmt.setNCharacterStream(1, null, 0);
    }
    @Test (expected = SQLException.class)
    public void test012_SetNClob() throws SQLException {
        pstmt.setNClob(1, new NClob() {
            @Override
            public long length() throws SQLException {
                return 0;
            }

            @Override
            public String getSubString(long pos, int length) throws SQLException {
                return null;
            }

            @Override
            public Reader getCharacterStream() throws SQLException {
                return null;
            }

            @Override
            public InputStream getAsciiStream() throws SQLException {
                return null;
            }

            @Override
            public long position(String searchstr, long start) throws SQLException {
                return 0;
            }

            @Override
            public long position(Clob searchstr, long start) throws SQLException {
                return 0;
            }

            @Override
            public int setString(long pos, String str) throws SQLException {
                return 0;
            }

            @Override
            public int setString(long pos, String str, int offset, int len) throws SQLException {
                return 0;
            }

            @Override
            public OutputStream setAsciiStream(long pos) throws SQLException {
                return null;
            }

            @Override
            public Writer setCharacterStream(long pos) throws SQLException {
                return null;
            }

            @Override
            public void truncate(long len) throws SQLException {

            }

            @Override
            public void free() throws SQLException {

            }

            @Override
            public Reader getCharacterStream(long pos, long length) throws SQLException {
                return null;
            }
        });
    }
    @Test (expected = SQLException.class)
    public void test103_SetNClob2() throws SQLException {
        pstmt.setNClob(1, null, 0);
    }

    @Test
    public void test104_SetBlobOK() throws SQLException {
        TestUtils.runInMain();
        pstmt.setBlob(1, new TDBlob(new byte[]{1, 2, 3, 4, 5}, true));
    }

    @Test (expected = SQLException.class)
    public void test104_SetBlobErr() throws SQLException {
        TestUtils.runIn336();
        pstmt.setBlob(1, new TDBlob(new byte[]{1, 2, 3, 4, 5}, true));
    }

    @Test (expected = SQLException.class)
    public void test105_SetBlob2() throws SQLException {
        pstmt.setBlob(1, null, 0);
    }

    @Test (expected = SQLException.class)
    public void test106_SetSQLXML() throws SQLException {
        pstmt.setSQLXML(1, null);
    }

    @Test (expected = SQLException.class)
    public void test107_SetObject() throws SQLException {
        pstmt.setObject(1, null, 0, 0);
    }

    @Test (expected = SQLException.class)
    public void test108_SetAsciiStream() throws SQLException {
        pstmt.setAsciiStream(1, null, 0);
    }

    @Test (expected = SQLException.class)
    public void test109_SetBinaryStream() throws SQLException {
        pstmt.setBinaryStream(1, null, 0);
    }

    @Test (expected = SQLException.class)
    public void test110_SetCharacterStream() throws SQLException {
        pstmt.setCharacterStream(1, null, 0);
    }

    @Test
    public void test111_SetTagNull() throws SQLException {
        TSWSPreparedStatement wsPreparedStatement = pstmt.unwrap(TSWSPreparedStatement.class);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.BOOLEAN);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.TINYINT);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.SMALLINT);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.INTEGER);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.BIGINT);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.FLOAT);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.DOUBLE);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.TIMESTAMP);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.BINARY);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.VARCHAR);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.VARBINARY);
        wsPreparedStatement.setTagSqlTypeNull(1, Types.NCHAR);
    }

    @Test
    public void test112_SetObject2() throws SQLException {
        TSWSPreparedStatement wsPreparedStatement = pstmt.unwrap(TSWSPreparedStatement.class);
        wsPreparedStatement.setObject(1, null, Types.BOOLEAN);
        wsPreparedStatement.setObject(1, null, Types.TINYINT);
        wsPreparedStatement.setObject(1, null, Types.SMALLINT);
        wsPreparedStatement.setObject(1, null, Types.INTEGER);
        wsPreparedStatement.setObject(1, null, Types.BIGINT);
        wsPreparedStatement.setObject(1, null, Types.FLOAT);
        wsPreparedStatement.setObject(1, null, Types.DOUBLE);
        wsPreparedStatement.setObject(1, null, Types.TIMESTAMP);
        wsPreparedStatement.setObject(1, null, Types.BINARY);
        wsPreparedStatement.setObject(1, null, Types.VARCHAR);
        wsPreparedStatement.setObject(1, null, Types.VARBINARY);
        wsPreparedStatement.setObject(1, null, Types.NCHAR);
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
        statement.execute("create table if not exists " + db_name + "." + tableName + " (ts timestamp, c1 int)");
        statement.close();

        pstmt = connection.prepareStatement("select 1");
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }

        if (pstmt != null){
            pstmt.close();
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