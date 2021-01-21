import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.codec.digest.DigestUtils;

public class TdEngineSuperDataGen {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        String url = "jdbc:TAOS://127.0.0.1:6030/test?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        // create database
        stmt.executeUpdate("create database if not exists hdb");
        // use database
        stmt.executeUpdate("use hdb");
        stmt.executeUpdate("drop table if exists sdata");
        // create table
        stmt.executeUpdate(
                "create table if not exists sdata (uptime timestamp, id int, x int , y int ,cmt binary(100)) tags(location nchar(100),tname nchar(100))");

        ZoneId zoneId = ZoneId.systemDefault();
        Map<String, String> table = new HashMap<>();
        table.put("dt001", "beijing");
        table.put("dt002", "shanghai");
        table.put("dt003", "chongqing");
        table.put("dt004", "xian");
        for (Entry<String, String> kv : table.entrySet()) {
            LocalDateTime d = LocalDateTime.now().minusMonths(2);
            long rowCount = LocalDateTime.now().atZone(zoneId).toEpochSecond() - d.atZone(zoneId).toEpochSecond();
            Random r = new Random();
            StringBuilder sb = null;
            long startTime = System.currentTimeMillis();
            try {
                for (long i = 0; i < rowCount; i++) {
                    sb = new StringBuilder("insert into " + kv.getKey() + " using sdata tags(" + kv.getValue() + "," + kv.getKey() + ") values('");
                    d = d.plusSeconds(1);
                    sb.append(d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.MS")));
                    sb.append("'," + i + "," + r.nextInt(100) + "," + r.nextInt(100) + ",'");
                    sb.append(DigestUtils.md5Hex(d.toString()));
                    sb.append("')");
                    System.out.println("SQL >>> " + sb.toString());
                    stmt.executeUpdate(sb.toString());
                }
            } catch (SQLException e) {
                System.out.println(d);
                System.out.println(sb.toString());
                e.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("generate data execute time:" + (endTime - startTime) + "ms, resultset rows " + rowCount
                    + ", " + rowCount * 1000 / (endTime - startTime) + " rows/sec");
        }
        stmt.close();
        conn.close();
    }

}
