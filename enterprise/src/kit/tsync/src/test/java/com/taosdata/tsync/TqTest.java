package com.taosdata.tsync;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class TqTest {

    private static final String TOPIC = "tq_test";
    private static Random random = new Random(System.currentTimeMillis());
    private static List<String> partitions = new ArrayList<>();

    public static void main(String[] args) throws SQLException {

        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_HOST, "master");
        props.setProperty(TSDBDriver.PROPERTY_KEY_PORT, "6041");
        props.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        props.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        props.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        props.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        TQueueProducer producer = new TQueueProducer(props);
        IntStream.range(1, 11).forEach(partition -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    ProducerRecord record = new ProducerRecord(
                            TOPIC,
                            partition,
                            new Person("name_" + i, random.nextInt(), random.nextBoolean()).toString()
                    );
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println(metadata);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        /**********************************************************************************************/
        /*
        Connection conn = DriverManager.getConnection("jdbc:TAOS-RS://:?", props);
        Statement stmt = conn.createStatement();
        // drop topic
        stmt.execute("drop topic if exists " + topic);
        // create topic
        stmt.execute("create topic if not exists " + topic + " partitions 10");
        // show topics
        ResultSet rs = stmt.executeQuery("show topics");
        printResult(rs);
        // show partitions
        stmt.execute("use " + topic);
        rs = stmt.executeQuery("show tables");
        while (rs.next()) {
            String partition = rs.getString("table_name");
            partitions.add(partition);
        }
        printResult(rs);

        // insert into partitions
        partitions.stream().forEach(partitionName -> {
            // insert into XXX(offset, ts, content) values(offset, ts, content);
            String sql = "insert into " + partitionName + "(off,ts,content) values(?,?,?)";
            try {
                PreparedStatement pstmt = conn.prepareStatement(sql);
                for (int offset = 0; offset < 100; offset++) {
                    pstmt.setLong(1, offset);
                    pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                    Person person = new Person("name_" + offset, random.nextInt(), random.nextBoolean());
//                    System.out.println(person.toString());
                    pstmt.setString(3, person.toString());
                    pstmt.execute();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
         */
    }

}
