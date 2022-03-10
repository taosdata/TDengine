package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TSDBJNIConnectorTest {

    private static final String host = "127.0.0.1";
    private static TSDBResultSetRowData rowData;

    @Test
    public void test() throws SQLException {
        // init
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
        TSDBJNIConnector.init(properties);

        // connect
        TSDBJNIConnector connector = new TSDBJNIConnector();
        connector.connect(host, 6030, null, "root", "taosdata");

        // setup
        String setupSqlStrs[] = {"create database if not exists d precision \"us\"",
                "create table if not exists d.t(ts timestamp, f int)",
                "create database if not exists d2",
                "create table if not exists d2.t2(ts timestamp, f int)",
                "insert into d.t values(now+100s, 100)",
                "insert into d2.t2 values(now+200s, 200)"
        };
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
//            System.out.println(columnMetaDataList.get(i));
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
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"};
        connector.insertLines(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);

        // close connection
        connector.executeQuery("drop database if exists d");
        connector.executeQuery("drop database if exists d2");
        connector.closeConnection();
    }

    private static boolean next(TSDBJNIConnector connector, long pSql) throws SQLException {
        if (rowData != null)
            rowData.clear();

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
        connector.connect(host, 6030, null, "root", "taosdata");
        connector.executeQuery("drop database if exists test");
        connector.executeQuery("create database if not exists test");
        connector.executeQuery("use test");
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
        connector.connect(host, 6030, null, "root", "taosdata");
        connector.executeQuery("drop database if exists test");
        connector.executeQuery("create database if not exists test");
        connector.executeQuery("use test");
        connector.executeQuery("create table weather(ts timestamp, f1 int) tags(t1 int)");

        // 1. init + prepare
        long stmt = connector.prepareStmt("insert into ? using weather tags(?) values(?,?)");

        long ts = System.currentTimeMillis();

        for (int ind_batch = 0; ind_batch < 10; ind_batch++) {

            ts += ind_batch * 1000 * 1000;
            System.out.println("batch: " + ind_batch + ", ts: " + ts);

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

    private void bind_col_timestamp(TSDBJNIConnector connector, long stmt, long ts_start, int numOfRows) throws SQLException {
        ByteBuffer colDataList = ByteBuffer.allocate(numOfRows * Long.BYTES);
        colDataList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> colDataList.putLong(ts_start + ind * 1000L));

        ByteBuffer lengthList = ByteBuffer.allocate(numOfRows * Long.BYTES);
        lengthList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> lengthList.putLong(Integer.BYTES));

        ByteBuffer isNullList = ByteBuffer.allocate(numOfRows * Integer.BYTES);
        isNullList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> isNullList.putInt(0));

        connector.bindColumnDataArray(stmt, colDataList, lengthList, isNullList, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, Long.BYTES, numOfRows, 0);
    }

    private void bind_col_integer(TSDBJNIConnector connector, long stmt, int numOfRows) throws SQLException {
        ByteBuffer colDataList = ByteBuffer.allocate(numOfRows * Integer.BYTES);
        colDataList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> colDataList.putInt(new Random().nextInt(100)));

        ByteBuffer lengthList = ByteBuffer.allocate(numOfRows * Long.BYTES);
        lengthList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> lengthList.putLong(Integer.BYTES));

        ByteBuffer isNullList = ByteBuffer.allocate(numOfRows * Integer.BYTES);
        isNullList.order(ByteOrder.LITTLE_ENDIAN);
        IntStream.range(0, numOfRows).forEach(ind -> isNullList.putInt(0));

        connector.bindColumnDataArray(stmt, colDataList, lengthList, isNullList, TSDBConstants.TSDB_DATA_TYPE_INT, Integer.BYTES, numOfRows, 1);
    }

    private void stmt_set_table_tags(TSDBJNIConnector connector, long stmt, String tbname) throws SQLException {
        ByteBuffer tagDataList = ByteBuffer.allocate(Integer.BYTES);
        tagDataList.order(ByteOrder.LITTLE_ENDIAN);
        tagDataList.putInt(new Random().nextInt(100));

        ByteBuffer typeList = ByteBuffer.allocate(1);
        typeList.order(ByteOrder.LITTLE_ENDIAN);
        typeList.put((byte) TSDBConstants.TSDB_DATA_TYPE_INT);

        ByteBuffer lengthList = ByteBuffer.allocate(1 * Long.BYTES);
        lengthList.order(ByteOrder.LITTLE_ENDIAN);
        lengthList.putLong(Integer.BYTES);

        ByteBuffer isNullList = ByteBuffer.allocate(1 * Integer.BYTES);
        isNullList.order(ByteOrder.LITTLE_ENDIAN);
        isNullList.putInt(0);

        connector.setBindTableNameAndTags(stmt, tbname, 1, tagDataList, typeList, lengthList, isNullList);
    }

}
