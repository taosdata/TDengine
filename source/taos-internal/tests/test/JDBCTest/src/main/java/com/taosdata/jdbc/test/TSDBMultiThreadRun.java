package com.taosdata.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSDBMultiThreadRun implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(TSDBMultiThreadRun.class);

	private final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private String TSDB_URL = "jdbc:TSDB://192.168.100.128:6200/tbase?user=root&password=taosdata";
	private String tableName = "test";

	private String txt40 = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
	private String txt36 = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

	private AtomicLong used = new AtomicLong(0);
	private AtomicLong insert = new AtomicLong(0);
	private int count;
	private String dbName;
	private String host;

	public TSDBMultiThreadRun(int i, int count, String host) {
		this.tableName = this.tableName + i;
		this.dbName = this.tableName;
		this.count = count;
		this.host = host;
		this.TSDB_URL = "jdbc:TSDB://" + this.host + ":6200/tbase?user=root&password=taosdata";
	}

	public void run() {
		doInsert();
	}

	private void doInsert() {

		Connection conn = null;
		Statement stmt = null;
		ResultSet resSet = null;
		try {
			Class.forName(TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				DriverManager.setLoginTimeout(3);
				conn = (Connection) DriverManager.getConnection(TSDB_URL); // create JDBC connection
				System.out.println("==========> a new connection build index=" + tableName);
			}
			stmt = (Statement) conn.createStatement(); // create statement
			stmt.setQueryTimeout(3);

			// insert
			long timestamp = 10000l;
			for (;;) {
				timestamp++;
				long now = System.currentTimeMillis();
				String sql = "insert into " + tableName + " values(" + timestamp + ",2,300,40,50.888,6,7,true,'" + txt40
						+ "','" + txt40 + "','" + txt40 + "','" + txt40 + "','" + txt40 + "','" + txt40 + "','" + txt40
						+ "','" + txt40 + "','" + txt40 + "','" + txt40 + "','" + txt40 + "','" + txt36 + "')";
				int in = stmt.executeUpdate(sql); // execute update operation
				if (in == 1) {
					insert.addAndGet(in);
				} else {
					logger.warn("table:" + tableName + "insert warn...." + in);
				}
				used.addAndGet(System.currentTimeMillis() - now);

				if (timestamp >= 50000000) {
					break;
				}
			}

		} catch (ClassNotFoundException e) {
			logger.error("", e);
		} catch (SQLException e) {
			logger.error("", e);
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			try {
				if (resSet != null) // close resultset once everything is done.
					resSet.close();
				if (stmt != null) // close statement
					stmt.close();
				if (conn != null) // close connection
					conn.close();
			} catch (SQLException e) {
				logger.error("", e);
			}
		}
	}

	public long getInsertCount() {
		return this.insert.get();
	}

	public long getCost() {
		return this.used.get();
	}

	public String getTableName() {
		return this.tableName;
	}
}
