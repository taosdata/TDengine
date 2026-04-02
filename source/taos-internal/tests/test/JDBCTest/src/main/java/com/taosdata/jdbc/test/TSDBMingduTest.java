package com.taosdata.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TSDBMingduTest {
	private static final String JDBC_PROTOCAL = "jdbc:TSDB://";

	private String host = "127.0.0.1";
	private String user = "root";
	private String password = "taosdata";
	private int port = 0;
	private String jdbcUrl = "";

	private String databaseName = "db";
	private String metricsName = "mt";
	private String tablePrefix = "t";

	private int tablesCount = 1000;
	private int rowsPerTable = 1000;
	private int threadNum = 1;
	private int insertMethod = 0;
	private int cache = 2000;
	private int ablocks = 40000;
	private int tables = 10000;
	private int batchSize = 1000;
	private long beginTimestamp = 1519833600000L;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TSDBMingduTest tester = new TSDBMingduTest();
		tester.doReadArgument(args);

		System.out.println("---------------------------------------------------------------");
		System.out.println("Start Testing...");
		System.out.println("---------------------------------------------------------------");

		tester.MakeJdbcUrl();

		TSDBMingduCreateSchema createSchema = new TSDBMingduCreateSchema(tester.jdbcUrl, tester.databaseName,
				tester.metricsName, tester.tablePrefix, tester.tablesCount, tester.cache, tester.ablocks,
				tester.tables);
		createSchema.run();

		int tablePerThread = tester.tablesCount / tester.threadNum;
		ExecutorService executorService = Executors.newFixedThreadPool(tester.threadNum);
		for (int i = 0; i < tester.threadNum; i++) {
			executorService.execute(new TSDBMingduInsertData(i, tester.insertMethod, tester.jdbcUrl,
					tester.databaseName, tester.tablePrefix, i * tablePerThread, (i + 1) * tablePerThread,
					tester.rowsPerTable, tester.batchSize, tester.beginTimestamp));
		}

		executorService.shutdown();
		while (!executorService.isTerminated()) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}

		System.out.println("---------------------------------------------------------------");
		System.out.println("All insert thread finished...");
		System.out.println("---------------------------------------------------------------");

		TSDBMingduQueryData queryData = new TSDBMingduQueryData(tester.jdbcUrl, tester.databaseName,
				tester.metricsName);
		queryData.run();

		System.out.println("---------------------------------------------------------------");
		System.out.println("Stop Testing...");
		System.out.println("---------------------------------------------------------------");
	}

	private void doReadArgument(String[] args) {
		System.out.println(
				"arguments format : tablesCount rowsPerTable threadNum insertMethod dbname cache ablocks tables host ");

		if (args.length >= 1) {
			this.tablesCount = Integer.parseInt(args[0]);
		}

		if (args.length >= 2) {
			this.rowsPerTable = Integer.parseInt(args[1]);
		}

		if (args.length >= 3) {
			this.threadNum = Integer.parseInt(args[2]);
		}

		if (args.length >= 4) {
			this.insertMethod = Integer.parseInt(args[3]);
		}
		
		if (args.length >= 5) {
			this.databaseName = args[4];
		}

		if (args.length >= 6) {
			this.cache = Integer.parseInt(args[5]);
		}

		if (args.length >= 7) {
			this.ablocks = Integer.parseInt(args[6]);
		}

		if (args.length >= 8) {
			this.tables = Integer.parseInt(args[7]);
		}

		if (args.length >= 9) {
			this.host = args[8];
		}

		if (this.insertMethod == 0) {
			this.rowsPerTable = (int) (Math.ceil((double) this.rowsPerTable / this.batchSize) * this.batchSize);
		} else {
			this.tablesCount = (int) (Math.ceil(
					(double) this.tablesCount / (this.threadNum * this.batchSize) * (this.threadNum * this.batchSize)));
		}

		System.out.printf(
				"arguments tablesCount:%d rowsPerTable:%d threadNum:%d, insertMethod:%s dbname:%s cache:%d ablocks:%d tables:%d host:%s \n",
				this.tablesCount, this.rowsPerTable, this.threadNum,
				this.insertMethod == 0 ? "sameTablePerBatch" : "differentTablePerBatch", this.databaseName, this.cache, this.ablocks,
				this.tables, this.host);
	}

	private void MakeJdbcUrl() {
		// jdbc:TSDB://192.168.0.1:0/dbname?user=root&password=taosdata
		this.jdbcUrl = String.format("%s%s:%d/%s?user=%s&password=%s", JDBC_PROTOCAL, this.host, this.port, "",
				this.user, this.password);
		System.out.println(this.jdbcUrl);
	}
}

class TSDBMingduCreateSchema {
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private String jdbcUrl;
	private String databaseName;
	private String metricsName;
	private String tablePrefix;
	private int tablesCount;
	private int cache;
	private int ablocks;
	private int tables;
	private Connection conn = null;

	public TSDBMingduCreateSchema(String jdbcUrl, String databaseName, String metricsName, String tablePrefix,
			int tablesCount, int cache, int ablocks, int tables) {
		this.jdbcUrl = jdbcUrl;
		this.databaseName = databaseName;
		this.metricsName = metricsName;
		this.tablePrefix = tablePrefix;
		this.tablesCount = tablesCount;
		this.cache = cache;
		this.ablocks = ablocks;
		this.tables = tables;
	}

	public void run() {
		this.ConnectToTaosd();
		this.CreateSchema();
		this.CloseConnection();
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
		System.out.printf("create schema, get connection from %s success\n", this.jdbcUrl);
	}

	private void CreateSchema() {
		Statement stmt = null;
		int start = (int) System.currentTimeMillis();
		try {
			stmt = (Statement) this.conn.createStatement();

			String sql = String.format("create database if not exists %s cache %d ablocks %d tblocks 500 tables %d",
					this.databaseName, this.cache, this.ablocks, this.tables);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format("use %s", this.databaseName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format(
					"create table if not exists %s (ts timestamp, value double) tags(collectNode binary(64), gatewayId binary(64), physicalDevice binary(64), resourceGroup binary(64), deviceResource binary(64))",
					this.metricsName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			for (int i = 0; i < this.tablesCount; i++) {
				sql = String.format("create table if not exists %s%d using mt tags('%d', '%d', '%d', '%d', '%d')",
						this.tablePrefix, i, i, i, i, i, i);
				stmt.executeUpdate(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("create schema failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("create schema failed");
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
		System.out.printf("Total %d tables created, time spend %d seconds.\n", this.tablesCount, (end - start) / 1000);
	}

	private void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class TSDBMingduInsertData implements Runnable {
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private int threadIndex;
	private String jdbcUrl;
	private int insertMethod;
	private String databaseName;
	private String tablePrefix;
	private int tablesBegin;
	private int tablesEnd;
	private int rowsPerTable;
	private int batchSize;
	private long beginTimestamp;

	private Connection conn = null;
	private long rowsInserted = 0;

	public TSDBMingduInsertData(int threadIndex, int insertMethod, String jdbcUrl, String databaseName,
			String tablePrefix, int tablesBegin, int tablesEnd, int rowsPerTable, int batchSize, long beginTimestamp) {
		this.threadIndex = threadIndex;
		this.insertMethod = insertMethod;
		this.jdbcUrl = jdbcUrl;
		this.databaseName = databaseName;
		this.tablePrefix = tablePrefix;
		this.tablesBegin = tablesBegin;
		this.tablesEnd = tablesEnd;
		this.rowsPerTable = rowsPerTable;
		this.batchSize = batchSize;
		this.beginTimestamp = beginTimestamp;
	}

	@Override
	public void run() {
		this.ConnectToTaosd();
		this.UseDb();
		if (this.insertMethod == 0) {
			this.ExecuteInsertSameTablePerBatch();
		} else {
			this.ExecuteInsertDifferentTablePerBatch();
		}
		this.CloseConnection();
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
		System.out.printf("Thread:%d, get connection from %s success\n", this.threadIndex, this.jdbcUrl);
	}

	private void UseDb() {
		Statement stmt = null;
		try {
			stmt = (Statement) this.conn.createStatement();
			String sql = String.format("use %s", this.databaseName);
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("use db failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("use db failed");
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

	private void ExecuteInsertSameTablePerBatch() {
		Statement stmt = null;
		int start = (int) System.currentTimeMillis();
		int loopCount = this.rowsPerTable / this.batchSize;
		long databaseTime = 0;

		try {
			stmt = (Statement) conn.createStatement();
			for (int loop = 0; loop < loopCount; loop++) {
				int end = (int) System.currentTimeMillis();
				System.out.printf("Thread:%d, loop:%d:%d, %d rows inserted, time spend %d seconds.\n", this.threadIndex,
						loop, loopCount, this.rowsInserted, (end - start) / 1000);

				for (int table = this.tablesBegin; table < this.tablesEnd; ++table) {
					StringBuffer buffer = new StringBuffer();
					buffer.append("insert into ").append(this.tablePrefix).append(table).append(" values");
					for (int batch = 0; batch < this.batchSize; ++batch) {
						int rows = loop * this.batchSize + batch;
						buffer.append("(").append(this.beginTimestamp + rows * 1000).append(",").append(rows)
								.append(")");
					}

					int tmp1 = (int) System.currentTimeMillis();
					int affectRows = stmt.executeUpdate(buffer.toString());
					databaseTime += ((int) System.currentTimeMillis() - tmp1);

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
		System.out.printf(
				"Thread:%d, total %d rows inserted, %d tables, database time %d seconds, total time spend %d seconds.\n",
				this.threadIndex, this.rowsInserted, (this.tablesEnd - this.tablesBegin), databaseTime / 1000,
				(end - start) / 1000);
	}

	private void ExecuteInsertDifferentTablePerBatch() {
		Statement stmt = null;
		int start = (int) System.currentTimeMillis();
		long databaseTime = 0;

		try {
			stmt = (Statement) conn.createStatement();
			for (int row = 0; row < this.rowsPerTable; ++row, this.beginTimestamp++) {
				if (row % this.batchSize == 0) {
					int end = (int) System.currentTimeMillis();
					System.out.printf("Thread:%d, batch:%d:%d, %d rows inserted, time spend %d seconds.\n",
							this.threadIndex, row / this.batchSize, this.rowsPerTable / this.batchSize,
							this.rowsInserted, (end - start) / 1000);
				}

				for (int table = tablesBegin; table < this.tablesEnd; table += this.batchSize) {
					StringBuffer sql = new StringBuffer("insert into ");
					for (int batch = table; batch < table + this.batchSize; ++batch) {
						sql.append(this.tablePrefix).append(batch).append(" values(").append(this.beginTimestamp)
								.append(",").append(row).append(")");
					}

					int tmp1 = (int) System.currentTimeMillis();
					int affectRows = stmt.executeUpdate(sql.toString());
					databaseTime += ((int) System.currentTimeMillis() - tmp1);

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
		System.out.printf(
				"Thread:%d, total %d rows inserted, %d tables, database time %d seconds, total time spend %d seconds.\n",
				this.threadIndex, this.rowsInserted, (this.tablesEnd - this.tablesBegin), databaseTime / 1000,
				(end - start) / 1000);
	}

	private void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class TSDBMingduQueryData {
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private String jdbcUrl;
	private String databaseName;
	private String metricsName;
	private Connection conn = null;

	public TSDBMingduQueryData(String jdbcUrl, String databaseName, String metricsName) {
		this.jdbcUrl = jdbcUrl;
		this.databaseName = databaseName;
		this.metricsName = metricsName;
	}

	public void run() {
		this.ConnectToTaosd();
		this.ExecuteQuery();
		this.CloseConnection();
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
		System.out.printf("query data, get connection from %s success\n", this.jdbcUrl);
	}

	private void ExecuteQuery() {
		Statement stmt = null;
		ResultSet resSet = null;
		try {
			stmt = (Statement) conn.createStatement();
			String sql = String.format("select count(value) as total from %s.%s", this.databaseName, this.metricsName);

			resSet = stmt.executeQuery(sql);
			if (resSet == null) {
				System.out.println(sql + " failed");
				System.exit(4);
			}

			ResultSetMetaData metaData = resSet.getMetaData();
			for (int column = 0; column < metaData.getColumnCount(); ++column) {
				System.out.println(
						column + ", " + metaData.getColumnName(column) + ", " + metaData.getColumnType(column) + ", "
								+ metaData.getColumnTypeName(column) + ", " + metaData.getColumnDisplaySize(column));
			}
			int queryCount = 0;
			while (resSet.next()) {
				StringBuffer strBuff = new StringBuffer();
				for (int col = 0; col < metaData.getColumnCount(); col++) {
					strBuff.append(metaData.getColumnName(col)).append(" = ").append(resSet.getObject(col)).append(" ");
				}
				System.out.println(strBuff.toString());
				queryCount++;
			}

			System.out.println(sql + " success, rows in resultset:" + queryCount);
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
	}

	private void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
