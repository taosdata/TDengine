package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TSDBJNIConnectorTest {

    private static String host = TestEnvUtil.getHost();
    private static int port = TestEnvUtil.getJniPort();
    private static TSDBResultSetRowData rowData;
    private static final String DB_NAME = TestUtils.camelToSnake(TSDBJNIConnectorTest.class);

    @BeforeClass
    public static void beforeClass() {
        String specifyHost = SpecifyAddress.getInstance().getJniUrl();
        if (specifyHost != null) {
            host = specifyHost;
        }
    }

    @Test
    public void test() throws SQLException {
        // init
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
        TSDBJNIConnector.init(properties);

        // connect
        TSDBJNIConnector connector = new TSDBJNIConnector();
        connector.connect(host, port, null, TestEnvUtil.getUser(), TestEnvUtil.getPassword());

        // setup
        String[] setupSqlStrs = {"create database if not exists d precision \"us\"", "create table if not exists d.t(ts timestamp, f int)", "create database if not exists d2", "create table if not exists d2.t2(ts timestamp, f int)", "insert into d.t values(now+100s, 100)", "insert into d2.t2 values(now+200s, 200)"};
        for (String setupSqlStr : setupSqlStrs) {
            long setupSql = connector.executeQuery(setupSqlStr);

            assertEquals(0, connector.getResultTimePrecision(setupSql));
            if (connector.isUpdateQuery(setupSql)) {
                connector.freeResultSet(setupSql);
            }
        }

        {
            long sqlObj1 = connector.executeQuery("select * from d2.t2");
            assertEquals(0, connector.getResultTimePrecision(sqlObj1));
            List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
            int code = connector.getSchemaMetaData(sqlObj1, columnMetaDataList);
            rowData = new TSDBResultSetRowData(columnMetaDataList.size());
            assertTrue(next(connector, sqlObj1));
            assertEquals(0, connector.getResultTimePrecision(sqlObj1));
            connector.freeResultSet(sqlObj1);
        }

        // executeQuery
        long pSql = connector.executeQuery("select * from d.t");

        if (connector.isUpdateQuery(pSql)) {
            connector.freeResultSet(pSql);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEQUERY);
        }

        assertEquals(1, connector.getResultTimePrecision(pSql));

        // get schema
        List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        int code = connector.getSchemaMetaData(pSql, columnMetaDataList);
        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        }
        if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        }
        if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
        }

        assertEquals(1, connector.getResultTimePrecision(pSql));
        int columnSize = columnMetaDataList.size();
        // print metadata
        for (int i = 0; i < columnSize; i++) {
            System.out.println(columnMetaDataList.get(i));
        }
        rowData = new TSDBResultSetRowData(columnSize);
        // iterate resultSet
        for (int i = 0; next(connector, pSql); i++) {
            assertEquals(1, connector.getResultTimePrecision(pSql));
        }
        // close resultSet
        code = connector.freeResultSet(pSql);
        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        }
        // close statement
        connector.executeQuery("use d");
        String[] lines = new String[]{"st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000", "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};
        connector.insertLines(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);

        // close connection
        connector.executeQuery("drop database if exists d");
        connector.executeQuery("drop database if exists d2");
        connector.closeConnection();
    }

    private static boolean next(TSDBJNIConnector connector, long pSql) throws SQLException {
        if (rowData != null) rowData.clear();

        int code = connector.fetchRow(pSql, rowData);
        if (code == TSDBConstants.JNI_CONNECTION_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        } else if (code == TSDBConstants.JNI_RESULT_SET_NULL) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_RESULT_SET_NULL);
        } else if (code == TSDBConstants.JNI_NUM_OF_FIELDS_0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_NUM_OF_FIELDS_0);
        } else return code != TSDBConstants.JNI_FETCH_END;
    }

    @Test
    public void param_bind_one_batch_multi_table() throws SQLException {
        TSDBJNIConnector connector = new TSDBJNIConnector();
        connector.connect(host, port, null, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        connector.executeQuery("drop database if exists " + DB_NAME);
        connector.executeQuery("create database if not exists " + DB_NAME);
        connector.executeQuery("use " + DB_NAME);
        connector.executeQuery("create table weather(ts timestamp, f1 int) tags(t1 int)");

        // 1. init + prepare
        long stmt = connector.prepareStmt("insert into ? using weather tags(?) values(?,?)");
        for (int i = 0; i < 10; i++) {
            // 2. set_tbname_tags
            stmt_set_table_tags(connector, stmt, "t" + i);
            // 3. bind_single_param_batch
            // bind timestamp
            long ts = System.currentTimeMillis();
            bind_col_timestamp(connector, stmt, ts, 100);
            // bind int
            bind_col_integer(connector, stmt, 100);
            // 4. add_batch
            connector.addBatch(stmt);
        }
        connector.executeBatch(stmt);
        connector.closeBatch(stmt);

        connector.executeQuery("drop database if exists test");

        connector.closeConnection();
    }

    @Test
    public void param_bind_multi_batch_multi_table() throws SQLException {
        TSDBJNIConnector connector = new TSDBJNIConnector();
        connector.connect(host, port, null, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        connector.executeQuery("drop database if exists " + DB_NAME);
        connector.executeQuery("create database if not exists " + DB_NAME);
        connector.executeQuery("use " + DB_NAME);
        connector.executeQuery("create table weather(ts timestamp, f1 int) tags(t1 int)");

        // 1. init + prepare
        long stmt = connector.prepareStmt("insert into ? using weather tags(?) values(?,?)");

        long ts = System.currentTimeMillis();

        for (int ind_batch = 0; ind_batch < 10; ind_batch++) {

            ts += ind_batch * 1000 * 1000;

            for (int i = 0; i < 10; i++) {
                // 2. set_tbname_tags
                stmt_set_table_tags(connector, stmt, "t" + i);
                // 3. bind_single_param_batch
                // bind timestamp

                bind_col_timestamp(connector, stmt, ts, 100);
                // bind int
                bind_col_integer(connector, stmt, 100);
                // 4. add_batch
                connector.addBatch(stmt);
            }
            connector.executeBatch(stmt);
        }

        connector.closeBatch(stmt);

        connector.executeQuery("drop database if exists test");

        connector.closeConnection();
    }

    @Test
    public void paramBindWithoutTableNameJNI() throws SQLException {
        TSDBJNIConnector connector = new TSDBJNIConnector();
        connector.connect(host, port, null, TestEnvUtil.getUser(), TestEnvUtil.getPassword());
        connector.executeQuery("drop database if exists " + DB_NAME);
        connector.executeQuery("create database if not exists " + DB_NAME);
        connector.executeQuery("use " + DB_NAME);
        connector.executeQuery("create table weather(ts timestamp, f1 int) tags(t1 int)");
        connector.executeQuery("create table t1 using weather tags (1)");

        // 1. init + prepare
        long stmt = connector.prepareStmt("insert into t1 values(?, ?)");
        for (int i = 0; i < 10; i++) {
            long ts = System.currentTimeMillis();
            bind_col_timestamp(connector, stmt, ts, 100);
            // bind int
            bind_col_integer(connector, stmt, 100);
            // 4. add_batch
            connector.addBatch(stmt);
        }
        connector.executeBatch(stmt);
        connector.closeBatch(stmt);

        connector.executeQuery("drop database if exists test");

        connector.closeConnection();
    }

    @Test
    public void paramBindWithoutTableName() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":" + port + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        Connection connection = DriverManager.getConnection(url);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop database if exists " + DB_NAME);
            statement.executeUpdate("create database if not exists " + DB_NAME);
            statement.executeUpdate("use " + DB_NAME);
            statement.executeUpdate("create table weather(ts timestamp, f1 int, f2 nchar(30)) tags(t1 int)");
            statement.executeUpdate("create table t1 using weather tags (2)");
        }

        String sql = "insert into t1 values(?, ?, ?)";
        try (TSDBPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSDBPreparedStatement.class)) {
            Random random = new Random(System.currentTimeMillis());
            for (int i = 0; i < 10; i++) {
                long current = System.currentTimeMillis();
                ArrayList<Long> tsList = new ArrayList<>();
                for (int j = 0; j < 100; j++) {
                    tsList.add(current + j);
                }
                statement.setTimestamp(0, tsList);

                ArrayList<Integer> f1List = new ArrayList<>();
                for (int j = 0; j < 100; j++) {
                    f1List.add(random.nextInt(Integer.MAX_VALUE));
                }
                statement.setInt(1, f1List);

                ArrayList<String> f2List = new ArrayList<>();
                for (int j = 0; j < 100; j++) {
                    f2List.add("California.LosAngeles");
                }
                statement.setNString(2, f2List, 30);

                statement.columnDataAddBatch();
            }
            statement.columnDataExecuteBatch();

            statement.executeUpdate("drop database if exists test");
        }
        connection.close();
    }

    @Test
    public void getTableVgID() throws SQLException {
        TSDBJNIConnector conn = new TSDBJNIConnector();
        conn.connect("localhost", port, null, TestEnvUtil.getUser(), TestEnvUtil.getPassword());

        conn.executeQuery("drop database if exists " + DB_NAME + "_get_table_vgroup_id");
        conn.executeQuery("create database if not exists " + DB_NAME + "_get_table_vgroup_id");
        conn.executeQuery("use " + DB_NAME + "_get_table_vgroup_id");
        conn.executeQuery("create table weather(ts timestamp, f1 int, f2 nchar(30)) tags(t1 int)");
        conn.executeQuery("create table t1 using weather tags (2)");

        int vgID = conn.getTableVGroupID(DB_NAME + "_get_table_vgroup_id", "t1");
        System.out.println(vgID);
    }

    private void bind_col_timestamp(TSDBJNIConnector connector, long stmt, long ts_start, int numOfRows) throws SQLException {
        ByteBuffer colDataList = ByteBuffer.allocate(numOfRows * Long.BYTES);
        colDataList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> colDataList.putLong(ts_start + ind * 1000L));

        ByteBuffer lengthList = ByteBuffer.allocate(numOfRows * Integer.BYTES);
        lengthList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> lengthList.putInt(Integer.BYTES));

        ByteBuffer isNullList = ByteBuffer.allocate(numOfRows * Byte.BYTES);
        isNullList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> isNullList.put((byte) 0));

        connector.bindColumnDataArray(stmt, colDataList, lengthList, isNullList, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, Long.BYTES, numOfRows, 0);
    }

    private void bind_col_integer(TSDBJNIConnector connector, long stmt, int numOfRows) throws SQLException {
        ByteBuffer colDataList = ByteBuffer.allocate(numOfRows * Integer.BYTES);
        colDataList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> colDataList.putInt(new Random().nextInt(100)));

        ByteBuffer lengthList = ByteBuffer.allocate(numOfRows * Integer.BYTES);
        lengthList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> lengthList.putInt(Integer.BYTES));

        ByteBuffer isNullList = ByteBuffer.allocate(numOfRows * Byte.BYTES);
        isNullList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> isNullList.put((byte) 0));

        connector.bindColumnDataArray(stmt, colDataList, lengthList, isNullList, TSDBConstants.TSDB_DATA_TYPE_INT, Integer.BYTES, numOfRows, 1);
    }

    private void stmt_set_table_tags(TSDBJNIConnector connector, long stmt, String tbname) throws SQLException {
        ByteBuffer tagDataList = ByteBuffer.allocate(Integer.BYTES);
        tagDataList.order(ByteOrder.LITTLE_ENDIAN);
        tagDataList.putInt(new Random().nextInt(100));

        ByteBuffer typeList = ByteBuffer.allocate(1);
        typeList.order(ByteOrder.LITTLE_ENDIAN);
        typeList.put((byte) TSDBConstants.TSDB_DATA_TYPE_INT);

        ByteBuffer lengthList = ByteBuffer.allocate(Integer.BYTES);
        lengthList.order(ByteOrder.LITTLE_ENDIAN);
        lengthList.putInt(Integer.BYTES);

        ByteBuffer isNullList = ByteBuffer.allocate(Byte.BYTES);
        isNullList.order(ByteOrder.LITTLE_ENDIAN);
        isNullList.put((byte) 0);

        connector.setBindTableNameAndTags(stmt, tbname, 1, tagDataList, typeList, lengthList, isNullList);
    }

    @Test
    public void testLoadLibrary() throws SQLException {
        if (System.getProperty("os.name").toLowerCase().contains("linux")) {
            System.setProperty("TD_LIBRARY_PATH", "/usr/lib/");
        } else if (System.getProperty("os.name").toLowerCase().contains("mac")) {
            System.setProperty("TD_LIBRARY_PATH", "/usr/local/lib/");
        } else {
            return;
        }
        // init

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
        TSDBJNIConnector.init(properties);
        System.setProperty("TD_LIBRARY_PATH", "");

    }
}

