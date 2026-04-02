package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;

import java.sql.*;
import java.util.Properties;

public class ImportTestTask extends TestTask {

    private Properties properties;

    public ImportTestTask() {
        this.properties = new Properties();
    }

    public ImportTestTask(Properties properties) {
        this.properties = properties;
    }

    public void run() {

        System.out.printf("\t%s started...\n", Thread.currentThread().getName());
        Connection connection = getConnection(properties);

        System.out.println("\tImport test");
        System.out.println("\tStarting ImportTest.importData...");

        importData(connection);
    }

    private void importData(Connection connection) throws TestFailureException {

        String threadName = Thread.currentThread().getName();
        String db = "db_" + threadName.replaceAll("-","");
        int replica = 1;
        String tb = "tb";
        String sql = "";

        System.out.printf("\t%s: ImportTest case: importData\n", threadName);
        Statement stmt;
        try {
            // create database with replica = 3
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
            String[] columns = {"ts timestamp", "c1 int", "c2 nchar(10)"};
            sql = SqlGenerator.getCreateTableSql(tb, columns);
            stmt.executeUpdate(sql);
            System.out.printf("\t%s: Table created.\n", threadName);

            // insert one record
            Thread.currentThread().sleep(3000);
            columns = new String[] {"'2018-10-10 11:10:00.000'", "1", "'a'"};
            sql = SqlGenerator.getSingleInsertSql(tb, columns);
            int res = stmt.executeUpdate(sql);
            columns = new String[] {"'2018-10-09 10:09:30.000'", "0", "'涛思'"};
            sql = SqlGenerator.getSingleImportSql(tb, columns);

            // import historical records
            long ts = 1539054000000l; // '2018-10-09 11:00:00.000'
            int imported = 0; // counter for imported records
            int batchNum = 100;
            int batchSize = 100;
            System.out.printf("\t%s: Start to import %d historical records...\n", threadName, batchNum * batchSize);
            long startTime = System.currentTimeMillis();
            System.out.printf("\t%s: Import started at time: %s\n", threadName, new Timestamp(startTime).toString());
            for (int i = 0; i < batchNum; i++) {
                StringBuilder importSql = new StringBuilder("import into ").append(tb).append(" values ");
                for (int j = 0; j < batchSize; j++ ) {
                    ts = ts + 1000;
                    importSql.append(" (").append(ts).append(", ").append(j + i * batchSize).append(", '涛思') ");
                }
                sql = importSql.toString();
                res = stmt.executeUpdate(sql);
                imported += res;
            }
            long endTime = System.currentTimeMillis();
            System.out.printf("\t%s: Importing completed!\n", threadName);
            System.out.printf("\t%s: Import completed at time: %s\n", threadName, new Time(endTime).toString());
            System.out.printf("\t%s: Total records imported: %d\n", threadName, imported);

            // query table
            int fetched = 0;
            sql = "select * from " + tb;
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                fetched++;
            }
            resultSet.close();
            System.out.printf("\t%s: Total records fetched: %d\n", threadName, fetched);
            if (fetched != 1 + batchNum * batchSize) {
                System.out.printf("\t%s: Failed: Total rows imported: %d; Number of rows fetched: %d\n", threadName, imported, fetched);
                sql = SqlGenerator.getDropDbSql(db);
                stmt.executeUpdate(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        }

        return;
    }
}
