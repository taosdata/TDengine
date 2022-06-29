package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

// ANCHOR: SQLWriter

/**
 * A helper class encapsulate the logic of writing using SQL.
 * <p>
 * The main interfaces are two methods:
 * <ol>
 *     <li>{@link SQLWriter#processLine}, which receive raw data from WriteTask and group raw data by table names.</li>
 *     <li>{@link  SQLWriter#flush}, which assemble INSERT statement and execute it.</li>
 * </ol>
 * <p>
 * There is a technical skill worth mentioning: we create table as needed when "table does not exist" error occur instead of creating table automatically using syntax "INSET INTO tb USING stb".
 * This ensure that checking table existence is a one-time-only operation.
 * </p>
 *
 * </p>
 */
public class SQLWriter {
    final static Logger logger = LoggerFactory.getLogger(SQLWriter.class);

    private Connection conn;
    private Statement stmt;

    /**
     * current number of buffered records
     */
    private int bufferedCount = 0;
    /**
     * Maximum number of buffered records.
     * Flush action will be triggered if bufferedCount reached this value,
     */
    private int maxBatchSize;

    /**
     * Map from table name to column values. For example:
     * "tb001" -> "(1648432611249,2.1,114,0.09) (1648432611250,2.2,135,0.2)"
     */
    private Map<String, String> tbValues = new HashMap<>();

    /**
     * Map from table name to tag values in the same order as creating stable.
     * Used for creating table.
     */
    private Map<String, String> tbTags = new HashMap<>();

    public SQLWriter(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }


    /**
     * Get Database Connection
     *
     * @return Connection
     * @throws SQLException
     */
    private static Connection getConnection() throws SQLException {
        String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
        return DriverManager.getConnection(jdbcURL);
    }

    /**
     * Create Connection and Statement
     *
     * @throws SQLException
     */
    public void init() throws SQLException {
        conn = getConnection();
        stmt = conn.createStatement();
        stmt.execute("use test");
    }

    /**
     * Convert raw data to SQL fragments, group them by table name and cache them in a HashMap.
     * Trigger writing when number of buffered records reached maxBachSize.
     *
     * @param line raw data get from task queue in format: tbName,ts,current,voltage,phase,location,groupId
     */
    public void processLine(String line) throws SQLException {
        bufferedCount += 1;
        int firstComma = line.indexOf(',');
        String tbName = line.substring(0, firstComma);
        int lastComma = line.lastIndexOf(',');
        int secondLastComma = line.lastIndexOf(',', lastComma - 1);
        String values = "(" + line.substring(firstComma + 1, secondLastComma) + ") ";
        if (tbValues.containsKey(tbName)) {
            tbValues.put(tbName, tbValues.get(tbName) + values);
        } else {
            tbValues.put(tbName, values);
        }
        if (!tbTags.containsKey(tbName)) {
            String location = line.substring(secondLastComma + 1, lastComma);
            String groupId = line.substring(lastComma + 1);
            String tagValues = "('" + location + "'," + groupId + ')';
            tbTags.put(tbName, tagValues);
        }
        if (bufferedCount == maxBatchSize) {
            flush();
        }
    }


    /**
     * Assemble INSERT statement using buffered SQL fragments in Map {@link SQLWriter#tbValues} and execute it.
     * In case of "Table does not exit" exception, create all tables in the sql and retry the sql.
     */
    public void flush() throws SQLException {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (Map.Entry<String, String> entry : tbValues.entrySet()) {
            String tableName = entry.getKey();
            String values = entry.getValue();
            sb.append(tableName).append(" values ").append(values).append(" ");
        }
        String sql = sb.toString();
        try {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            // convert to error code defined in taoserror.h
            int errorCode = e.getErrorCode() & 0xffff;
            if (errorCode == 0x362 || errorCode == 0x218) {
                // Table does not exist
                createTables();
                stmt.executeUpdate(sql);
            } else {
                throw e;
            }
        }
        tbValues.clear();
        bufferedCount = 0;
    }

    /**
     * Create tables in batch using syntax:
     * <p>
     * CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
     * </p>
     */
    private void createTables() throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        for (String tbName : tbValues.keySet()) {
            String tagValues = tbTags.get(tbName);
            sb.append("IF NOT EXISTS ").append(tbName).append(" USING meters TAGS ").append(tagValues).append(" ");
        }
        String sql = sb.toString();
        stmt.executeUpdate(sql);
    }

    public boolean hasBufferedValues() {
        return bufferedCount > 0;
    }

    public int getBufferedCount() {
        return bufferedCount;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }
}
// ANCHOR_END: SQLWriter
