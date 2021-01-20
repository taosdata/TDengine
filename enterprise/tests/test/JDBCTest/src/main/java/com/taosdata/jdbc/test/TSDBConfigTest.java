package com.taosdata.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class TSDBConfigTest {
	private static final String JDBC_PROTOCAL = "jdbc:TSDB://";
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";

	private String host = "ubuntu";
	private String user = "root";
	private String password = "taosdata";
	private int port = 0;
	private String jdbcUrl = "";

	private String databaseName = "db";
	private String metricsName = "mt";
	private String tablePrefix = "t";
	private long beginTimestamp = 1519833600000L;

	private Connection conn = null;

	private int columns = 10;
	private int rowsize = 100;
	private int tblocks = 500;
	private int ablocks = 1000;
	private int cache = 100000;
	private int tables = 1000;
	private int tablesCount = 1000;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TSDBConfigTest tester = new TSDBConfigTest();
		tester.doReadArgument(args);

		System.out.println("---------------------------------------------------------------");
		System.out.println("Starting fast Testing...");
		System.out.println("---------------------------------------------------------------");

		tester.MakeJdbcUrl();
		tester.ConnectToTaosd();
		tester.CreateDbAndTable();
		tester.ExecuteInsert();
		tester.CloseConnection();

		System.out.println("---------------------------------------------------------------");
		System.out.println("Stop fast Testing...");
		System.out.println("---------------------------------------------------------------");
	}

	private void doReadArgument(String[] args) {
		System.out.println("arguments format : columns rowsize tablesCount");

		if (args.length >= 1) {
			this.columns = Integer.parseInt(args[0]);
		}

		if (args.length >= 2) {
			this.rowsize = Integer.parseInt(args[1]);
		}

		if (args.length >= 3) {
			this.tblocks = Integer.parseInt(args[2]);
		}

		if (args.length >= 4) {
			this.ablocks = Integer.parseInt(args[3]);
		}

		if (args.length >= 5) {
			this.cache = Integer.parseInt(args[4]);
		}

		if (args.length >= 6) {
			this.tables = Integer.parseInt(args[5]);
		}

		if (args.length >= 7) {
			this.tablesCount = Integer.parseInt(args[6]);
		} else {
			this.tablesCount = this.tables;
		}

		System.out.printf("arguments columns:%d rowsize:%d tablesCount:%d \n",
				this.columns, this.rowsize, this.tablesCount);
	}

	private void MakeJdbcUrl() {
		// jdbc:TSDB://ubuntu:0/dbname?user=root&password=taosdata
		this.jdbcUrl = String.format("%s%s:%d/%s?user=%s&password=%s", JDBC_PROTOCAL, this.host, this.port, "",
				this.user, this.password);
		System.out.println(this.jdbcUrl);
	}

	private void ConnectToTaosd() {
		try {
			Class.forName(TSDB_DRIVER);
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("get connection from " + this.jdbcUrl + " failed");
			System.exit(4);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("get connection from " + this.jdbcUrl + " failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("get connection from " + this.jdbcUrl + " failed");
			System.exit(4);
		} finally {
		}
		System.out.println("get connection from " + this.jdbcUrl + " success");
	}

	private void CreateDbAndTable() {
		Statement stmt = null;
		try {
			stmt = (Statement) this.conn.createStatement();

			String sql = String.format("create database if not exists %s replica 1 days 10 keep 3650", this.databaseName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format("use %s", this.databaseName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			StringBuilder buffer = new StringBuilder();
			buffer.append("create table if not exists ").append(this.metricsName).append("(ts timestamp");
			for (int i = 1; i < this.columns - 1; ++i) {
				buffer.append(",f").append(i).append(" binary(2)");
			}
			int remainSize = this.rowsize - 8 - (this.columns - 2) * 2;
			buffer.append(",f").append(this.columns - 1).append(" binary(").append(remainSize).append(")) tags(t1 int)");
			stmt.executeUpdate(buffer.toString());
			System.out.println(buffer.toString() + " success");

			for (int i = 0; i < this.tablesCount; i++) {
				sql = String.format("create table if not exists %s%d using mt tags('%d')", this.tablePrefix, i, i);
				stmt.executeUpdate(sql);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("create db and table failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("create db and table failed");
			System.exit(4);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("create db and table success");
	}

	public void ExecuteInsert() {
		Statement stmt = null;
		try {
			stmt = (Statement) conn.createStatement();
			StringBuffer sql = new StringBuffer("insert into");
			for (int table = 0; table < this.tablesCount; table++) {
				sql.append(String.format(" %s%s values(%d", this.tablePrefix, table, this.beginTimestamp));
				for (int i = 1; i < this.columns; ++i) {
					sql.append(",'1'");
				}
				sql.append(")");
				
				if (sql.length() > 30000) {
					stmt.executeUpdate(sql.toString());
					stmt.executeUpdate(sql.toString());
					sql.delete(0, sql.length());
					sql.append("insert into");
				}
			}
		} catch (

		SQLException e) {
			e.printStackTrace();
			System.out.println("insert into table failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("insert into table failed");
			System.exit(4);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
