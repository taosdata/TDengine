package com.taosdata.iot.glodon; /**
 * @author Jiangyi Hou
 * @since 19-1-29
 */

//import com.taosdata.jdbc.TSDBDriver;
//import com.taosdata.jdbc.TSDBResultSetRowData;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.util.Properties;

import java.util.HashSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This is a demo class for use cases of Glodon application development
 */

public class GlodonCasesDemo {

    private static final String JDBC_URL = "jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata";
    private static final String JDBC_DRIVER = "com.taosdata.jdbc.TSDBDriver";

    public static void main(String[] args) {

        String str = "2019-01-09T14:21:47.000Z";
        System.out.println(str.replaceAll(Pattern.compile("[TZtz]").pattern(), " "));
        System.out.println(str.replaceAll("t|z|T|Z", " "));


        HashSet<String> set1 = new HashSet<>();
        set1.add(new String("ab"));
        set1.add(new String("cd"));
        HashSet<String> set2 = new HashSet<>();
        set2.add(new String("cd"));
        set2.add(new String("ab"));

        System.out.println(set1.equals(set2));

        String itemValues = "1,2,3,";
        System.out.println(itemValues.split(",").length);
        Stream.of(itemValues.split(",")).forEach(System.out::println);

        String sql = "select * from  tbname order by ts asc";
        String sql1 = "select * from  tbname where ts > 0 order by ts asc";
        parse(sql);
        parse(sql1);



        String lineTxt = "'2019-02-23 18:59:17.000 ','1e9c1bd8-379d-11e9-9e2e-0242ac120005';_state#'ONLINE',_u#'189.9',_pf#'0.99',_online#'1',_i#'0.97',_switchOn#'100',_planStatus#'50',_copDate#'2019-02-24 03:00:13',_isSwitchOn#'true',_ap#'182.36',_lampPower#'100.0'\n";

        StringBuilder sqlBuilder = new StringBuilder("insert into ").append("tbname").append(" values ");
        String[] values = lineTxt.split(";");
        String tsMsgid = values[0];
        TreeSet<String> itemValuePairs = new TreeSet<String>();
        for(String pair : values[1].split(",")) {
            itemValuePairs.add(pair);
        }
//                        Collections.sort(itemValuePairs);

        sqlBuilder.append("(").append(tsMsgid);
        for (String pair : itemValuePairs) {
            sqlBuilder.append(",").append(pair.split("#")[1]);
        }
        sqlBuilder.append(") ");
        System.out.println(sqlBuilder.toString());
    }

    static void parse(String sql) {
        String tableName;
        String selectClause;
        String whereClause;
        String sqlTail;
        if (sql == null || sql.length() < 1) {
            System.out.println("ERROR: empty sql");
        } else {
            int fromPos = sql.toLowerCase().indexOf(" from ");
            int wherePos = sql.toLowerCase().indexOf(" where ");
            if (wherePos < 0) {
                tableName = sql.substring(fromPos + 5).trim().split(" ")[0];
                selectClause = sql.substring(0, fromPos + 5);
                whereClause = "";
                sqlTail = sql.substring(sql.toLowerCase().indexOf(tableName) + tableName.length());
            } else {
                tableName = sql.substring(fromPos + 5, wherePos).trim();
                selectClause = sql.substring(0, fromPos + 5);
                whereClause = "where ";
                sqlTail = sql.substring(wherePos + 5);
            }
            System.out.printf("tbname='%s'\n", tableName);
            System.out.printf("selectClause='%s'\n", selectClause);
            System.out.printf("whereClause='%s'\n", whereClause);
            System.out.printf("sqlTail='%s'\n", sqlTail);
        }
    }

//        Connection connection = null;
//        try {
//            Class.forName(JDBC_DRIVER);
//            Properties properties = new Properties();
//            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
//            properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
//            connection = DriverManager.getConnection(JDBC_URL, properties);
//
//            if (connection != null && !connection.isClosed()) {
//
//                /*********************************************************
//                 *
//                 * Implement A Time Window Using JDBC
//                 *
//                 *********************************************************/
//
//                // Params for a time window:
//                // String stableName = "stb";
//                // String[] tableNames = new String[] {"tb1", "tb2", "tb3"};
//                // long start = 1548700000000L;
//                // long end = 1548800000000L;
//                // String dbName = "db0";
//                // String timeWindowName = dbName + ".timeWindowResultTable"; //db0.timeWindowResultTable
//
//                // Create a sql string with given params in a time window:
//                String sql = "select c1, c2 into db0.timeWindow0 from stb where ts >= 1548700000000 and ts <= 1548800000000 and tbname in ('tb1', 'tb2', 'tb3') group by tbname";
//                // Execute the sql above to search for the desired data, meanwhile creating new table(s) behind the
//                // scene to store the query result set. If the sql statement has a "group by " clause, then multiple
//                // tables should be created to store the result set of each group respectively.
//                // For example, in the above sql, three tables should be created to store the max(c1) and avg(c2) of
//                // 'tb1', 'tb2', and 'tb3'. The three new result set tables should belong to a stable named by the
//                // given window name, i.e. "db0.timeWindowResultTable".
//                Statement stmt = connection.createStatement();
//                int res = stmt.executeUpdate(sql.toString());
//
//                // Now querying the result sets can be simply done by querying the stable "db0.timeWindowResultTable",
//                // just the same as querying a regular table. However, the column names in the result set table
//                // 'db0.timeWindowResultTable' will be the same as the original tables column names, or the alias
//                // specified by the user.
//                // For example, to retrieve all the result data that are generated from 'tb1'
//                ResultSet resultSet;
//                resultSet = stmt.executeQuery("select * from db0.timeWindowResultTable where tbname = 'tb1'");
//                // or to find the maximum of of max_c1 and avg_c2
//                resultSet = stmt.executeQuery("select max(c1), min(c2) from db0.timeWindowResultTable");
//
//
//                /*********************************************************
//                 *
//                 * Implement A Data Window Using JDBC
//                 *
//                 *********************************************************/
//
//                // Params for a time window:
//                // String[] tables = new String[] {"tb1", "tb2", "tb3"};
//                // long start = 1548700000000L;
//                // long count = 5000;
//                // String dbName = "db0";
//                // String dataWindowName = dbName + ".dataWindowResultTable"; //db0.dataWindowResultTable
//
//                // Create a sql string with give params in a data window:
//                String sql_forward_query = "select c1, c2 into db0.dataWindow0 from stb where ts >= 1548700000000 group by tbname order by ts asc limit 5000";
//                String sql_backward_query = "select c1, c2 into db0.dataWindow0 from stb where ts <= 1548700000000 group by tbname order by ts desc limit 5000";
//                // Similar to how a time window is implemented, the data window implementation is also a sql which will
//                // directly write the query result sets into new table(s).
//                res = stmt.executeUpdate(sql_backward_query.toString());
//                resultSet = stmt.executeQuery("select max(c1), min(c2) from db0.dataWindow0");
//                res = stmt.executeUpdate(sql_forward_query.toString());
//                resultSet = stmt.executeQuery("select max(c1), min(c2) from db0.dataWindow0");
//
//                /*********************************************************
//                 *
//                 * Implement A Subscription to A Window Using JDBC
//                 *
//                 *********************************************************/
//
//                // subscription
//                // blocked synchronized polling from result set table
//                String host = "192.168.0.1";
//                String user = "root";
//                String password = "taosdata";
//                String db = "db0";
//                String topic = "dataWindow0";
//                long startTime = 1548700000000L; // unix timestamp
//                int period = 1000; // in milliseconds
//
//                Consumer consumer = new TaosConsumer();
//                consumer.subscribe(subscription);
//                TSDBResultSetRowData rowData = null;
//                while(true) {
//                    rowData = consumer.consume(subscription);
//                    if (rowData != null) {
//                        System.out.println(rowData.getData().toString());
//                    } else {
//                        break;
//                    }
//                    try {
//                        Thread.sleep(1000);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
