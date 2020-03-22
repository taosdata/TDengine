import java.sql.*;

public class TSDBSyncSample {
    private static final String JDBC_PROTOCAL = "jdbc:TAOS://";
    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";

    private String host = "127.0.0.1";
    private String user = "root";
    private String password = "taosdata";
    private int port = 0;
    private String jdbcUrl = "";

    private String databaseName = "db";
    private String metricsName = "mt";
    private String tablePrefix = "t";

    private int tablesCount = 1;
    private int loopCount = 2;
    private int batchSize = 10;
    private long beginTimestamp = 1519833600000L;

    private long rowsInserted = 0;

    static {
        try {
            Class.forName(TSDB_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        TSDBSyncSample tester = new TSDBSyncSample();
        tester.doReadArgument(args);

        System.out.println("---------------------------------------------------------------");
        System.out.println("Start testing...");
        System.out.println("---------------------------------------------------------------");

        tester.doMakeJdbcUrl();
        tester.doCreateDbAndTable();
        tester.doExecuteInsert();
        tester.doExecuteQuery();

        System.out.println("\n---------------------------------------------------------------");
        System.out.println("Stop testing...");
        System.out.println("---------------------------------------------------------------");
    }

    private void doReadArgument(String[] args) {
        System.out.println("Arguments format: host tables loop batchs");
        if (args.length >= 1) {
            this.host = args[0];
        }

        if (args.length >= 2) {
            this.tablesCount = Integer.parseInt(args[1]);
        }

        if (args.length >= 3) {
            this.loopCount = Integer.parseInt(args[2]);
        }

        if (args.length >= 4) {
            this.batchSize = Integer.parseInt(args[3]);
        }
    }

    private void doMakeJdbcUrl() {
        // jdbc:TSDB://127.0.0.1:0/dbname?user=root&password=taosdata
        System.out.println("\nJDBC URL to use:");
        this.jdbcUrl = String.format("%s%s:%d/%s?user=%s&password=%s", JDBC_PROTOCAL, this.host, this.port, "",
                this.user, this.password);
        System.out.println(this.jdbcUrl);
    }

    private void doCreateDbAndTable() {
        System.out.println("\n---------------------------------------------------------------");
        System.out.println("Start creating databases and tables...");
        String sql = "";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()){

            sql = "create database if not exists " + this.databaseName;
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

            sql = "use " + this.databaseName;
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

            sql = "create table if not exists " + this.metricsName + " (ts timestamp, v1 int) tags(t1 int)";
            stmt.executeUpdate(sql);
            System.out.printf("Successfully executed: %s\n", sql);

            for (int i = 0; i < this.tablesCount; i++) {
                sql = String.format("create table if not exists %s%d using %s tags(%d)", this.tablePrefix, i,
                        this.metricsName, i);
                stmt.executeUpdate(sql);
                System.out.printf("Successfully executed: %s\n", sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.printf("Failed to execute SQL: %s\n", sql);
            System.exit(4);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
        System.out.println("Successfully created databases and tables");
    }

    public void doExecuteInsert() {
        System.out.println("\n---------------------------------------------------------------");
        System.out.println("Start inserting data...");
        int start = (int) System.currentTimeMillis();
        StringBuilder sql = new StringBuilder("");
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()){
            stmt.executeUpdate("use " + databaseName);
            for (int loop = 0; loop < this.loopCount; loop++) {
                for (int table = 0; table < this.tablesCount; ++table) {
                    sql = new StringBuilder("insert into ");
                    sql.append(this.tablePrefix).append(table).append(" values");
                    for (int batch = 0; batch < this.batchSize; ++batch) {
                        int rows = loop * this.batchSize + batch;
                        sql.append("(").append(this.beginTimestamp + rows).append(",").append(rows).append(")");
                    }
                    int affectRows = stmt.executeUpdate(sql.toString());
                    this.rowsInserted += affectRows;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.printf("Failed to execute SQL: %s\n", sql.toString());
            System.exit(4);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
        int end = (int) System.currentTimeMillis();
        System.out.println("Inserting completed!");
        System.out.printf("Total %d rows inserted, %d rows failed, time spend %d seconds.\n", this.rowsInserted,
                this.loopCount * this.batchSize - this.rowsInserted, (end - start) / 1000);
    }

    public void doExecuteQuery() {
        System.out.println("\n---------------------------------------------------------------");
        System.out.println("Starting querying data...");
        ResultSet resSet = null;
        StringBuilder sql = new StringBuilder("");
        StringBuilder resRow = new StringBuilder("");
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()){
            stmt.executeUpdate("use " + databaseName);
            for (int i = 0; i < this.tablesCount; ++i) {
                sql = new StringBuilder("select * from ").append(this.tablePrefix).append(i);

                resSet = stmt.executeQuery(sql.toString());
                if (resSet == null) {
                    System.out.println(sql + " failed");
                    System.exit(4);
                }

                ResultSetMetaData metaData = resSet.getMetaData();
                System.out.println("Retrieve metadata of " + tablePrefix + i);
                for (int column = 1; column <= metaData.getColumnCount(); ++column) {
                    System.out.printf("Column%d: name = %s, type = %d, type name = %s, display size = %d\n", column, metaData.getColumnName(column), metaData.getColumnType(column),
                            metaData.getColumnTypeName(column), metaData.getColumnDisplaySize(column));
                }
                int rows = 0;
                System.out.println("Retrieve data of " + tablePrefix + i);
                while (resSet.next()) {
                    resRow = new StringBuilder();
                    for (int col = 1; col <= metaData.getColumnCount(); col++) {
                        resRow.append(metaData.getColumnName(col)).append("=").append(resSet.getObject(col))
                                .append(" ");
                    }
                    System.out.println(resRow.toString());
                    rows++;
                }

                try {
                    if (resSet != null)
                        resSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                System.out.printf("Successfully executed query: %s;\nTotal rows returned: %d\n", sql.toString(), rows);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.printf("Failed to execute query: %s\n", sql.toString());
            System.exit(4);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
        System.out.println("Query completed!");
    }

}
