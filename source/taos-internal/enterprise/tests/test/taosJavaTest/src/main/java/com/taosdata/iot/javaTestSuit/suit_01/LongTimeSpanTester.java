package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Jiangyi Hou
 * @since 19-5-8
 */
public class LongTimeSpanTester {
    private static final String HOST_IP = "192.168.1.21";
    public static void main(String[] args) {
        LongTimeSpanTester tester = new LongTimeSpanTester();
        System.out.println("Create db");
        tester.createDB();
        System.out.println("Auto create table");
        tester.testInsert_autoCreateTable();
        System.out.println("Insert");
        tester.testInsert();
    }

    public void createDB() {
//        Connection conn = new ConnectionFactory().getConnection(HOST_IP, new Properties());
        Connection conn = new ConnectionFactory().getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("drop database db");
            stmt.executeUpdate("create database db");
            stmt.executeUpdate("use db");
            stmt.executeUpdate("create table car (ts timestamp, c1 float, c2 float) tags(t1 int)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testInsert_autoCreateTable(){

        Connection conn = new ConnectionFactory().getConnection();
//        Connection conn = new ConnectionFactory().getConnection(HOST_IP, new Properties());
        Statement stmt = null;

        try {

//            stmt = (Statement) this.conn.createStatement();
            stmt = conn.createStatement();

            long time=1519833600000L;

            String stable="car";

            long start = System.currentTimeMillis();
            stmt.executeUpdate("use db");
            stmt.executeUpdate("create table strm as select count(*), avg(c1) from car interval(10s) sliding(5s)");

            long rows = 0L;
            for(int i=0;i<2;i++){

                String tablename="car_D000"+i;

                start = System.currentTimeMillis();

                for(int j=1;j<=100000;j++){

//                    rows = System.currentTimeMillis()+j;
                    rows = rows + j;

                    StringBuffer buffer = new StringBuffer();

                    buffer.append("insert into db.").append(tablename).append(" using db.").append(stable)

                            .append(" tags(").append(i).append(") values ");

//					buffer.append("insert into db.").append(tablename).append(" values ");

                    buffer.append("(").append(rows).append(",").append(Math.random() * 10).append(",")

                            .append(Math.random() * 10).append(")");

//					System.out.println(buffer.toString());

                    int affectRows = stmt.executeUpdate(buffer.toString());
                    if (j % 10 == 0) {
                        String sql = "import into db." + tablename + " values (" + (time -3 * j) + ", -" + (j/10) + ", -" + (j/10) + ")";
                        stmt.executeUpdate(sql);
                    }

//					System.out.println("≥…π¶£∫"+affectRows);

//					Thread.sleep(300);

                }

                System.out.println("time used: " + (System.currentTimeMillis() - start) + "ms");

            }



        } catch (SQLException e) {

            e.printStackTrace();

            System.out.println("insert into table failed");

            System.exit(4);

        } catch (Exception e) {

            e.printStackTrace();

            System.out.println("insert into table failed");

            System.exit(4);

        } finally {

            try {

                if (stmt != null) {
                    stmt.close();
                    conn.close();
                }

            } catch (SQLException e) {

                e.printStackTrace();

            }

        }

    }



    public void testInsert(){

        Statement stmt = null;
        Connection conn = new ConnectionFactory().getConnection();
//        Connection conn = new ConnectionFactory().getConnection(HOST_IP, new Properties());

        try {

//            stmt = (Statement) this.conn.createStatement();
            stmt = (Statement) conn.createStatement();
            stmt.executeUpdate("use db");
            stmt.executeUpdate("create table t0 (ts timestamp, c1 bigint)");

            long time=1519833800000l;

            String sql="insert into db.t0 values("+(time+3)+",3)";

            System.out.println(sql);

            int affectRows = stmt.executeUpdate(sql);

            System.out.println("Affected rows:"+affectRows);

        } catch (SQLException e) {

            e.printStackTrace();

            System.out.println("insert into table failed");

            System.exit(4);

        } catch (Exception e) {

            e.printStackTrace();

            System.out.println("insert into table failed");

            System.exit(4);

        } finally {

            try {

                if (stmt != null){
                    stmt.close();
                    conn.close();
                }

            } catch (SQLException e) {

                e.printStackTrace();

            }

        }

    }
}
