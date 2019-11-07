/**********************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies. 
 *  No part of this file may be reproduced, stored, transmitted, 
 *  disclosed or used in any form or by any means other than as 
 *  expressly provided by the written permission from TAOS Technologies
 *
 * *********************************************************************/

package com.taosdata.jdbc.test.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 *
 * @author Shengliang Guan
 * 
 *         Mar 20, 2018
 */

public class TSDBSyncSample {
	private static final String JDBC_PROTOCAL = "jdbc:TSDB://";
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

	private Connection conn = null;
	private long rowsInserted = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TSDBSyncSample tester = new TSDBSyncSample();
		tester.doReadArgument(args);

		System.out.println("---------------------------------------------------------------");
		System.out.println("Starting Testing...");
		System.out.println("---------------------------------------------------------------");

		tester.doMakeJdbcUrl();
		tester.doConnectToTaosd();
		tester.doCreateDbAndTable();
		tester.doExecuteInsert();
		tester.doExecuteQuery();
		tester.doCloseConnection();

		System.out.println("---------------------------------------------------------------");
		System.out.println("Stop Testing...");
		System.out.println("---------------------------------------------------------------");
	}

	private void doReadArgument(String[] args) {
		System.out.println("arguments format : host tables loop batchs");
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
		this.jdbcUrl = String.format("%s%s:%d/%s?user=%s&password=%s", JDBC_PROTOCAL, this.host, this.port, "",
				this.user, this.password);
		System.out.println(this.jdbcUrl);
	}

	private void doConnectToTaosd() {
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

	private void doCreateDbAndTable() {
		Statement stmt = null;
		try {
			stmt = (Statement) this.conn.createStatement();

			String sql = String.format("create database if not exists %s", this.databaseName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format("use %s", this.databaseName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format("create table if not exists %s (ts timestamp, v1 int) tags(t1 int)", this.metricsName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			for (int i = 0; i < this.tablesCount; i++) {
				sql = String.format("create table if not exists %s%d using %s tags(%d)", this.tablePrefix, i,
						this.metricsName, i);
				stmt.executeUpdate(sql);
				System.out.println(sql + " success");
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

	public void doExecuteInsert() {
		Statement stmt = null;
		int start = (int) System.currentTimeMillis();
		try {
			stmt = (Statement) this.conn.createStatement();
			for (int loop = 0; loop < this.loopCount; loop++) {
				for (int table = 0; table < this.tablesCount; ++table) {
					StringBuffer buffer = new StringBuffer();
					buffer.append("insert into ").append(this.tablePrefix).append(table).append(" values");
					for (int batch = 0; batch < this.batchSize; ++batch) {
						int rows = loop * this.batchSize + batch;
						buffer.append("(").append(this.beginTimestamp + rows).append(",").append(rows).append(")");
					}
					int affectRows = stmt.executeUpdate(buffer.toString());
					this.rowsInserted += affectRows;
				}
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
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		int end = (int) System.currentTimeMillis();
		System.out.printf("Total %d rows inserted, %d rows failed, time spend %d seconds.\n", this.rowsInserted,
				this.loopCount * this.batchSize - this.rowsInserted, (end - start) / 1000);
	}

	public void doExecuteQuery() {
		Statement stmt = null;
		ResultSet resSet = null;
		try {
			stmt = (Statement) this.conn.createStatement();
			for (int i = 0; i < this.tablesCount; ++i) {
				String sql = "select * from " + this.tablePrefix + i;

				resSet = stmt.executeQuery(sql);
				if (resSet == null) {
					System.out.println(sql + " failed");
					System.exit(4);
				}

				ResultSetMetaData metaData = resSet.getMetaData();
				for (int column = 1; column <= metaData.getColumnCount(); ++column) {
					System.out.println(i + ", " + metaData.getColumnName(column) + ", " + metaData.getColumnType(column)
							+ ", " + metaData.getColumnTypeName(column) + ", " + metaData.getColumnDisplaySize(column));
				}
				int queryCount = 0;
				while (resSet.next()) {
					StringBuffer strBuff = new StringBuffer();
					for (int col = 1; col <= metaData.getColumnCount(); col++) {
						strBuff.append(metaData.getColumnName(col)).append("=").append(resSet.getObject(col))
								.append(" ");
					}
					System.out.println(strBuff.toString());
					queryCount++;
				}

				System.out.println(sql + " success, querycount:" + queryCount);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("query table failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("query table failed");
			System.exit(4);
		} finally {
			try {
				if (resSet != null)
					resSet.close();
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("query table finished");
	}

	public void doCloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
