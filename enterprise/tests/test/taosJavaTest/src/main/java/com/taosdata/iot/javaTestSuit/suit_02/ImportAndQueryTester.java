package com.taosdata.iot.javaTestSuit.suit_02;

import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Random;

/**
 * Tester class for Fanuc case
 * @author Jiangyi Hou
 * @since 19-4-24
 */
public class ImportAndQueryTester {

    private static int rows = 10000;
    private static int loop = 10000;
    private static int tbNum = 1000;

    public static void main(String[] args) {
        if (args.length < 1) {
            // use default values
        } else if (args.length < 2){
            rows = Integer.valueOf(args[0]);
        } else {
            rows = Integer.valueOf(args[0]);
            loop = Integer.valueOf(args[1]);
        }

        ImportAndQueryTester tester = new ImportAndQueryTester();
        tester.runTest();

    }

    public void runTest() {
        Thread importThread = new Thread(new ImportTask(rows));
        Thread queryThread = new Thread(new QueryTask(loop, "select last_row(*) from mt_fanuc_cnc"));
        Thread countThread = new Thread(new CountTask(5000));
        Connection connection = new ConnectionFactory().getConnection();
        try {
            // set up db
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists fanuc_cnc");
            stmt.executeUpdate("create database fanuc_cnc");
            stmt.executeUpdate("use fanuc_cnc");
//            stmt.executeUpdate("create table tb (ts timestamp, c1 bigint)");
            stmt.executeUpdate("Create table mt_fanuc_cnc(" +
                    "dev_time TIMESTAMP," +
                    "cnc_id  BINARY(32)," +
                    "cnc_alarm_msg  NCHAR(225)," +
                    "cnc_spin_rate  SMALLINT," +
//                    "dtu_id  BINARY(32)," +
                    "cnc_cur_tool  SMALLINT," +
                    "dtu_4g_ip  BINARY(20)," +
                    "cnc_spin_actf  INT," +
                    "cnc_spin_load  FLOAT," +
                    "cnc_conn  SMALLINT," +
                    "cnc_spin_sets  INT," +
                    "cnc_cycletime  INT," +
                    "cnc_alarm_type  SMALLINT," +
                    "cnc_stats_stdbymode  SMALLINT," +
                    "cnc_alarm_no  INT," +
                    "cnc_worktime  INT," +
                    "cnc_stats_alarm  SMALLINT," +
                    "cnc_spin_acts  INT," +
                    "cnc_partsnum  INT," +
                    "cnc_spin_temp  SMALLINT," +
                    "cnc_cuttime  INT," +
                    "cnc_mprog  INT," +
                    "cnc_mcomt  BINARY(64)," +
                    "cnc_powertime  INT," +
                    "dtu_sim_iccid  BINARY(20)," +
                    "cnc_spin_setf  INT," +
                    "cnc_cprog  INT," +
                    "cnc_x_load  FLOAT," +
                    "cnc_ip  BINARY(20)," +
                    "cnc_cncmode  SMALLINT," +
                    "cnc_partsall  INT," +
                    "cnc_stats_aut  SMALLINT," +
                    "cnc_stats_emg  SMALLINT," +
                    "cnc_autorunsts  SMALLINT," +
                    "cnc_cutrate  SMALLINT," +
                    "cnc_x_temp  SMALLINT," +
                    "subcmd  BINARY(32)" +
                    ") tags (dtu_id  BINARY(32))");
            for (int i = 0; i <= tbNum; i++) {
                stmt.executeUpdate("create table tb" + i + " using mt_fanuc_cnc tags(" + i +")");
            }
            importThread.start();
            queryThread.start();
            countThread.start();
            queryThread.join();
            countThread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println("Importing and querying all completed!");
    }

    class ImportTask implements Runnable {
        private int rows = 10000;
        ImportTask(int rows) {
            this.rows = rows;
        }
        public void run() {
            Connection connection = new ConnectionFactory().getConnection();
            Random random = new Random();
            StringBuilder sql = new StringBuilder();
            try {
                System.out.printf("Import-thread starts to import %d rows into each table\n", rows);
                Statement stmt = connection.createStatement();
                // create db and tables
                stmt.executeUpdate("use fanuc_cnc");
                long dev_time = 1537146000000L;
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < tbNum; j++) {
                        sql = new StringBuilder("import into tb" + j + " values (");
//                        dev_time = dev_time + (random.nextInt(100) - 6);
                        dev_time = dev_time + (random.nextInt(100) + 1);
                        sql.append(dev_time).append(",").append(i).append(" , '樱木花道', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)");
                        stmt.executeUpdate(sql.toString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("Failure when executing %s\n", sql.toString());
            } finally {
                System.out.println("Import completed!");
            }
        }
    }

    class QueryTask implements Runnable {
        private int loop = 10000;
        private String sql = "show databases";
        QueryTask(int loop, String sql) {
            this.loop = loop;
            this.sql = sql;
        }
        public void run() {
            long sleep = 1000;
            Connection connection = new ConnectionFactory().getConnection();
            try {
                System.out.printf("Query-thread starts to query all data from tb for %d times, sleeping %d ms between two queries\n", loop, sleep);
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("use fanuc_cnc");
                for (int i = 0; i < this.loop; i++) {
                    ResultSet resultSet = stmt.executeQuery(sql);
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    resultSet.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Failure during querying");
            } finally{
                System.out.println("Query completed!");
            }
        }
    }

    class CountTask implements Runnable {
        private long sleep =  2000L;

        CountTask(long sleep) {
            this.sleep = sleep;
        }

        public void run() {
            Connection connection = new ConnectionFactory().getConnection();
            long count = 0;
            long ts0 = System.currentTimeMillis();
            BigDecimal speed = new BigDecimal(0);

            try {
                Thread.currentThread().sleep(3000);
                System.out.println("Count-thread starts counting inserted records and calculating writing speed...");
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("use fanuc_cnc");
                for (int i = 1; i <= 1000; i++) {
                    ResultSet resultSet = stmt.executeQuery("select count(*) from mt_fanuc_cnc");
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    if (resultSet.next()) {
                        count = resultSet.getLong(1) - count;
                    }
//                    speed = BigDecimal.valueOf(System.currentTimeMillis() - ts0).divide(BigDecimal.valueOf(count*1000000));
                    speed = BigDecimal.valueOf(count * 1000).divide(BigDecimal.valueOf(System.currentTimeMillis() - ts0), RoundingMode.HALF_UP);
                    ts0 = System.currentTimeMillis();
                    System.out.printf("Count=%d, current insert speed=%f rows/s\n", resultSet.getLong(1), speed);
                    Thread.currentThread().sleep(sleep);
                    resultSet.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("Count-thread job completed!");
            }
        }
    }
}
