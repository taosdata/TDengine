package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;
import com.taosdata.iot.javaTestSuit.utils.Timer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class SimpleInsertTestTask extends TestTask{
    private Properties properties;

    public SimpleInsertTestTask() {
        this.properties = new Properties();
    }

    public SimpleInsertTestTask(Properties properties) {
        this.properties = properties;
    }

    public void run() {
        System.out.printf("\t%s started SimpleInsertTestTask...\n", Thread.currentThread().getName());
        Connection connection = getConnection(properties);
        insertData(connection);
    }

    private void insertData(Connection connection) throws TestFailureException {

        String threadName = Thread.currentThread().getName();
        String db = "db_" + threadName.replaceAll("-","");
        int replica = 1;
        String tb = "tb";
        String sql = "";
        Timer timer = new Timer();

        Statement stmt;
        try {
            // create database with replica = 1
            System.out.printf("\t%s: Creating database...\n", threadName);
            stmt = connection.createStatement();
            sql = SqlGenerator.getDropDbSql(db);
            stmt.executeUpdate(sql); // drop db if exists
            sql = SqlGenerator.getCreateDbSql(db, replica);
            stmt.executeUpdate(sql); // create db
            sql = "use " + db;
            stmt.executeUpdate(sql);

            // create table
            System.out.printf("\t%s: Creating table...\n", threadName);
//            String[] columns = {"ts timestamp", "c1 int", "c2 nchar(10)"};
//            String[] columns = {"ts timestamp", "speed bigint", "str binary(128)"};
            String[] columns = {"ts timestamp", "speed int", "str int"};
            sql = SqlGenerator.getCreateTableSql(tb, columns);
            stmt.executeUpdate(sql);
            System.out.printf("\t%s: Table created.\n", threadName);

            // import historical records
//            long ts = 1539054000000l; // '2018-10-09 11:00:00.000'
            long ts = 1430000000000l;
            int inserted = 0; // counter for imported records
            int batchNum = 100000;
            int batchSize = 1;
            int res = 0;
            long start = 0l;
            long end = 0l;
            long time = 0l;
            System.out.printf("\t%s: Start to insert %d records...\n", threadName, batchNum * batchSize);

            for (int i = 0; i < batchNum; i++) {
                StringBuilder insertSql = new StringBuilder("insert into ").append(tb).append(" values ");
                for (int j = 0; j < batchSize; j++) {
                    ts = ts + 1000;
//                    insertSql.append(" (").append(ts).append(", ").append(j + i * batchSize).append(", '涛思') ");
                    insertSql.append(" (").append(ts).append(", ").append(j + i * batchSize).append(", 'dem0') ");
                }
                sql = insertSql.toString();
                timer.start();
                res = stmt.executeUpdate(sql);
                timer.stop();
                inserted += res;
            }
            System.out.printf("\t%s: Insert completed!\n", threadName);
            System.out.printf("\t%s: Total records imported: %d\n", threadName, inserted);
            System.out.printf("\t%s: Total time used for insert: %fs\n", threadName, timer.getTimeInSeconds());
            timer.reset();

            // query table
            System.out.printf("\t%s: Fetch records from table %s\n", threadName, tb);
            int fetched = 0;
            sql = "select * from " + tb;
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                fetched++;
            }
            resultSet.close();
            System.out.printf("\t%s: Total records fetched: %d\n", threadName, fetched);
            if (fetched != batchNum * batchSize) {
                System.out.printf("%s: Failed: Total rows inserted: %d; Number of rows fetched: %d\n", threadName, inserted, fetched);
                sql = SqlGenerator.getDropDbSql(db);
                stmt.executeQuery(sql);
            }
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        }

        return;
    }
}
