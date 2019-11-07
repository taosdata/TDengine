package com.taosdata.iot.javaTestSuit.utils;

import com.taosdata.iot.javaTestSuit.Exceptions.TaosSyntaxException;
import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;

/**
 * This utility class is created to help generate commonly used TSDB SQL strings
 */
public class SqlGenerator {

    public static String getCreateDbSql(String db) {

        StringBuilder sql = new StringBuilder("create database if not exists ").append(db);
        return sql.toString();
    }


    public static String getCreateDbSql(String db, int replica) {

        StringBuilder sql = new StringBuilder("create database if not exists ").append(db).append(" replica ").append(replica);
        return sql.toString();
    }

    public static String getCreateDbSql(String db, Integer replica, Integer days, Integer keep, Integer rows, Integer cache, Double ablocks,
                                 Integer tblocks, Integer tables, Integer ctime, Integer clog, Integer comp) throws TaosSyntaxException{

        StringBuilder sql = new StringBuilder("create database if not exists ").append(db);

        if (replica != null) {
            sql.append(" replica ").append(replica);
        }
        if (days != null) {
            sql.append(" days ").append(days);
        }
        if (keep != null) {
            sql.append(" keep ").append(keep);
        }
        if (rows != null) {
            sql.append(" rows ").append(rows);
        }
        if (cache != null) {
            sql.append(" cache ").append(cache);
        }
        if (ablocks != null) {
            sql.append(" ablocks ").append(ablocks);
        }
        if (tblocks != null) {
            sql.append(" tblocks ").append(tblocks);
        }
        if (tables != null) {
            sql.append(" tables ").append(tables);
        }
        if (ctime != null) {
            sql.append(" ctime ").append(ctime);
        }
        if (clog != null) {
            sql.append(" clog ").append(clog);
        }
        if (comp != null) {
            sql.append(" comp ").append(comp);
        }

        return sql.toString();
    }

    public static String getCreateDbSql(String db, Integer replica, Integer days, Integer keep, Integer rows, Integer cache, Double ablocks,
                                        Integer tblocks, Integer tables, Integer ctime, Integer clog, Integer comp, String precision) throws TaosSyntaxException{

        StringBuilder sql = new StringBuilder("create database if not exists ").append(db);

        if (replica != null) {
            sql.append(" replica ").append(replica);
        }
        if (days != null) {
            sql.append(" days ").append(days);
        }
        if (keep != null) {
            sql.append(" keep ").append(keep);
        }
        if (rows != null) {
            sql.append(" rows ").append(rows);
        }
        if (cache != null) {
            sql.append(" cache ").append(cache);
        }
        if (ablocks != null) {
            sql.append(" ablocks ").append(ablocks);
        }
        if (tblocks != null) {
            sql.append(" tblocks ").append(tblocks);
        }
        if (tables != null) {
            sql.append(" tables ").append(tables);
        }
        if (ctime != null) {
            sql.append(" ctime ").append(ctime);
        }
        if (clog != null) {
            sql.append(" clog ").append(clog);
        }
        if (comp != null) {
            sql.append(" comp ").append(comp);
        }
        if (precision != null) {
            sql.append(" precision ").append(precision);
        }

        return sql.toString();
    }

    /**
     * Generate sql string for "create table ..."
     * @param table table name
     * @param columns column names and column types. E.g. "s timestamp", "column1 int" ...
     * @return
     */
    public static String getCreateTableSql1(String table, String...columns) throws TaosSyntaxException{

        StringBuilder sql = new StringBuilder("create table ").append(table).append(" (");
        int colNum = columns.length;
        if (colNum < 1) {
            throw new TaosSyntaxException();
        }
        for (int i = 0; i < colNum - 1; i++) {
            sql.append(columns[i]).append(", ");
        }
        sql.append(columns[colNum - 1]).append(")");
        return sql.toString();
    }

    /**
     * Generate sql string for "create table ..."
     * @param table table name
     * @param columns column names and types. E.g. {"ts timestamp", "column1 int", "column2 bigint"}
     * @return
     */
    public static String getCreateTableSql(String table, String columns[]) throws TaosSyntaxException{

        StringBuilder sql = new StringBuilder("create table ").append(table).append(" (");
        int colNum = columns.length;

        if (colNum < 1) {
            throw new TaosSyntaxException();
        }
        for (int i = 0; i < colNum - 1; i++) {
            sql.append(columns[i]).append(", ");
        }
        sql.append(columns[colNum - 1]).append(")");
        return sql.toString();

    }

    /**
     *
     * Generate sql string for "create table using metric ..."
     * @param table
     * @param metric
     * @param tags
     * @return
     * @throws TaosSyntaxException
     */
    public static String getCreateTableUsingMetricSql(String table, String metric, String[] tags) throws TaosSyntaxException {

        StringBuilder sql = new StringBuilder("create table ").append(table).append(" using ").append(metric).append(" tags (");
        int tagNum = tags.length;

        if (tagNum < 1) {
            throw new TaosSyntaxException("Number of tags can't be 0");
        }

        for (int i = 0; i < tagNum - 1; i++) {
            sql.append(tags[i]).append(", ");
        }
        sql.append(tags[tagNum - 1]).append(")");
        return sql.toString();
    }

    /**
     *
     * Generate sql string for "create table ..." with specified row size (in bytes) and number of columns
     * This method will create a table with only binary type columns, the aim is to satisfy the desired row size
     * and column number.
     * @param tb table name
     * @param rowSize total length of all fields in table (in bytes)
     * @param columns number of columns in table
     * @return
     */
    public static String getCreateTableSql(String tb, int rowSize, int columns) throws TaosSyntaxException{

        if (rowSize <= 8) {
            throw new TaosSyntaxException("Row size can not be smaller than 9 bytes");
        }

        if (columns <= 1) {
            throw new TaosSyntaxException("At least 2 columns are required");
        }

        int singleColSize = (rowSize - 8) / (columns - 1);
        if (singleColSize < 1) {
            throw new TestFailureException("Average column size is smaller than 1 byte");
        }

        StringBuilder sql = new StringBuilder("create table ").append(tb).append(" (ts timestamp, ");
        for (int i = 1; i < columns - 1; i++) {
            sql.append(" c").append(i).append(" binary(").append(singleColSize).append(") ");
        }
        sql.append("c").append(columns - 1).append(" binary(").append((rowSize - 8) % singleColSize + singleColSize)
                .append("))");
        return sql.toString();
    }

    /**
     * Generate query string for "create metric ..."
     * @param metric
     * @param columns
     * @param tags
     * @return
     * @throws TaosSyntaxException
     */
    public static String getCreateMetricSql(String metric, String columns[], String tags[]) throws TaosSyntaxException {

        String sqlPrefix = getCreateTableSql(metric, columns);
        StringBuilder sql = new StringBuilder(sqlPrefix).append(" tags (");
        int tagNum = tags.length;

        if (tagNum < 1) {
            throw new TaosSyntaxException();
        }
        for (int i = 0; i < tagNum - 1; i++) {
            sql.append(tags[i]).append(", ");
        }
        sql.append(tags[tagNum - 1]).append(")");
        return sql.toString();

    }

    public static String getDropDbSql(String db) {

        StringBuilder sql = new StringBuilder("drop database if exists ").append(db);
        return sql.toString();
    }

    /**
     * Generate sql string for "insert into ..."
     * @param table
     * @param values
     * @return
     * @throws TaosSyntaxException
     */
    public static String getSingleInsertSql (String table, String values[]) throws TaosSyntaxException {

        if (values == null) {
            throw new TaosSyntaxException();
        }

        StringBuilder sql = new StringBuilder("insert into ").append(table).append(" values (");
        int valNum = values.length;

        if (valNum < 1) {
            throw new TaosSyntaxException("SQL query has syntax error!");
        }
        for (int i = 0; i < valNum - 1; i++) {
            sql.append(values[i]).append(", ");
        }
        sql.append(values[valNum - 1]).append(")");
        return sql.toString();

    }

    /**
     * Generate sql string for single import
     * @param table
     * @param values
     * @return
     * @throws TaosSyntaxException
     */
    public static String getSingleImportSql (String table, String values[]) throws TaosSyntaxException {

        String sql = getSingleInsertSql(table, values);
        sql = sql.replaceFirst("insert", "import");
        return sql;

    }

}
