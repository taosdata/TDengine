package com.taosdata.jdbc;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TSDBJNIConnectorTest {

    private static TSDBResultSetRowData rowData;

    @Test
    public void test() {
        try {

            try {
                //change sleepSeconds when debugging with attach to process to find PID
                int sleepSeconds = -1;
                if (sleepSeconds>0) {
                    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
                    String jvmName = runtimeBean.getName();
                    long pid = Long.valueOf(jvmName.split("@")[0]);
                    System.out.println("JVM PID  = " + pid);

                    Thread.sleep(sleepSeconds*1000);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            // init
            TSDBJNIConnector.init("/etc/taos", null, null, null);

            // connect
            TSDBJNIConnector connector = new TSDBJNIConnector();
            connector.connect("127.0.0.1", 6030, null, "root", "taosdata");

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
                System.out.println(columnMetaDataList.get(i));
            }
            rowData = new TSDBResultSetRowData(columnSize);
            // iterate resultSet
            for (int i = 0; next(connector, pSql); i++) {
                assertEquals(1, connector.getResultTimePrecision(pSql));
                System.out.println();
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
            String[] lines = new String[] {"st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
                                           "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000ns"};
            connector.insertLines(lines);

            // close connection
            connector.closeConnection();

        } catch (SQLWarning throwables) {
            throwables.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
        } else if (code == TSDBConstants.JNI_FETCH_END) {
            return false;
        } else {
            return true;
        }
    }

}
