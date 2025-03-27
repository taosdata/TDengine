package com.taos.example;

import com.taosdata.jdbc.TSDBDriver;
import org.locationtech.jts.util.Assert;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

// ANCHOR: efficient_writing
public class WSHighVolumeDemo {

    // modify host to your own
    private static final String HOST = "127.0.0.1";
    private static final int port = 6041;
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int NUM_OF_SUB_TABLE = 10000;
    private static final int NUM_OF_ROW = 10;

    public static void main(String[] args) throws SQLException {

        String url = "jdbc:TAOS-WS://" + HOST + ":" + port + "/?user=root&password=taosdata";
        Properties properties = new Properties();
        // Use an efficient writing mode
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");
        // The maximum number of rows to be batched in a single write request
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "10000");
        // The maximum number of rows to be cached in the queue (for each backend write
        // thread)
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, "100000");
        // Number of backend write threads
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "5");
        // Enable this option to automatically reconnect when the connection is broken
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        // The maximum time to wait for a write request to be processed by the server in
        // milliseconds
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");
        // Enable this option to copy data when modifying binary data after the
        // `addBatch` method is called
        properties.setProperty(TSDBDriver.PROPERTY_KEY_COPY_DATA, "false");
        // Enable this option to check the length of the sub-table name and the length
        // of variable-length data types
        properties.setProperty(TSDBDriver.PROPERTY_KEY_STRICT_CHECK, "false");

        try (Connection conn = DriverManager.getConnection(url, properties)) {
            init(conn);

            // If you are certain that the child table exists, you can avoid binding the tag
            // column to improve performance.
            String sql = "INSERT INTO power.meters (tbname, groupid, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                long current = System.currentTimeMillis();

                int rows = 0;
                for (int j = 0; j < NUM_OF_ROW; j++) {
                    // To simulate real-world scenarios, we adopt the approach of writing a batch of
                    // sub-tables, with one record per sub-table.
                    for (int i = 1; i <= NUM_OF_SUB_TABLE; i++) {
                        pstmt.setString(1, "d_" + i);

                        pstmt.setInt(2, i);
                        pstmt.setString(3, "location_" + i);

                        pstmt.setTimestamp(4, new Timestamp(current + j));
                        pstmt.setFloat(5, random.nextFloat() * 30);
                        pstmt.setInt(6, random.nextInt(300));
                        pstmt.setFloat(7, random.nextFloat());

                        // when the queue of backend cached data reaches the maximum size, this method
                        // will be blocked
                        pstmt.addBatch();
                        rows++;
                    }

                    pstmt.executeBatch();

                    if (rows % 50000 == 0) {
                        // The semantics of executeUpdate in efficient writing mode is to synchronously
                        // retrieve the number of rows written between the previous call and the current
                        // one.
                        int affectedRows = pstmt.executeUpdate();
                        Assert.equals(50000, affectedRows);
                    }
                }
            }
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to insert to table meters using efficient writing, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }

    private static void init(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS power");
            stmt.execute("USE power");
            stmt.execute(
                    "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        }
    }
}
// ANCHOR_END: efficient_writing
