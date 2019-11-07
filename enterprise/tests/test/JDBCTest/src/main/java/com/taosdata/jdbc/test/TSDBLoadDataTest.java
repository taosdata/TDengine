package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;

public class TSDBLoadDataTest {

	private String host = "192.168.0.1";
	private String configDir = "/etc/taos";
	private String user = "root";
	private String password = "taosdata";
	private String jdbcUrl = "";
	private String dbName = "evidev";
	private String tablePrefix = "device";

	private String startTime = "2018-5-1 0:0:0";
	private String endTime = "2018-5-2 0:0:0";

	private Connection conn = null;
	private int startIdx = 0;
	private int endIdx = 0;

	public void MakeJdbcUrl() {
		String JDBC_PROTOCAL = "jdbc:TSDB://";
		int port = 0;
		this.jdbcUrl = JDBC_PROTOCAL + host + ":" + port + "/" + dbName + "?user=" + user + "&password=" + password;
		System.out.println(this.jdbcUrl);
	}

	public void SetHost(String host) {
		this.host = host;
	}

	public void setDB(String db) {
		this.dbName = db;
		this.tablePrefix = this.dbName + "." + this.tablePrefix;
	}

	public void setTimeRange(String startTime, String endTime) {
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public void ConnectTbase() {
//		TSDBJNIConnector.init(this.configDir, "", "");
		Properties info = new Properties();
		info.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, this.configDir);

		String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
		try {
			Class.forName(TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				conn = (Connection) DriverManager.getConnection(this.jdbcUrl, info);
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

	public void ExecuteQuery() {
		Statement stmt = null;
		ResultSet reSet = null;
		try {
			stmt = (Statement) conn.createStatement();
			stmt.executeQuery("use " + this.dbName);

			for (int i = this.startIdx; i < this.endIdx; ++i) {
				String sql = "select * from " + tablePrefix + i
						+ " where receive_time<'%s' and receive_time>='%s' order by receive_time asc";
				sql = String.format(sql, this.endTime, this.startTime);

				System.out.println("Execute SQL: " + sql);

				reSet = stmt.executeQuery(sql);
				if (reSet == null) {
					System.out.println(sql + " failed");
					System.exit(4);
				}

				// ResultSetMetaData metaData = reSet.getMetaData();
				int numOfRows = 0;
				while (reSet.next()) {
					numOfRows++;
					// resSet.getString(columnIndex);
				}

				System.out.println(sql + " success, fetch rows:" + numOfRows);
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
				if (reSet != null)
					reSet.close();
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Query table data finished");
	}

	void setQueryMeterRange(int start, int end) {
		this.startIdx = start;
		this.endIdx = end;
	}

	public static class Tasks implements Runnable {
		private TSDBLoadDataTest test = new TSDBLoadDataTest();
		private int idleTime = 0;

		public void setQueryRange(int start, int end) {
			test.setQueryMeterRange(start, end);
		}

		public void setHost(String hostIp) {
			test.SetHost(hostIp);
		}

		public void setDB(String db) {
			test.setDB(db);
		}

		public void setIdleTime(int time) {
			this.idleTime = time;
		}

		public void setTimeRange(String startTime, String endTime) {
			test.setTimeRange(startTime, endTime);
		}

		public void run() {
			test.MakeJdbcUrl();
			test.ConnectTbase();

			while (true) {
				Long startTime = System.currentTimeMillis();
				test.ExecuteQuery();
				Long elapsed = System.currentTimeMillis() - startTime;
				System.out.println("Total elapsed time: " + elapsed + " ms\nSleep:" + this.idleTime + " sec.\n");

				try {
					Thread.sleep(this.idleTime * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		if (args.length < 6) {
			System.out.println(
					"Usage:\nloaddata [ip address] [db] [numOfThead] [numOfTable] [startime] [endtime] [idletime]");
			System.exit(-1);
		}

		String IPAddr = args[0];
		String db = args[1];

		int numOfThreads = Integer.parseInt(args[2]);
		if (numOfThreads > 100) {
			numOfThreads = 100;
			System.out.println("maximum threads to load data is set to 100");
		}

		int numOfMeters = Integer.parseInt(args[3]);
		if (numOfMeters > 50000) {
			numOfMeters = 50000;
			System.out.println("maximum number of meters is no larger than 50000");
		}

		String startTime = args[4];
		String endTime = args[5];

		int idleTime = Integer.parseInt(args[6]);
		int metersPerThread = numOfMeters / numOfThreads;

		System.out.println("Meters Per Thread: " + String.valueOf(metersPerThread));

		for (int i = 0; i < numOfThreads; i++) {
			Tasks t = new Tasks();
			t.setQueryRange(i * metersPerThread + 1, (i + 1) * metersPerThread + 1);
			t.setHost(IPAddr);
			t.setDB(db);

			t.setTimeRange(startTime, endTime);
			t.setIdleTime(idleTime);
			new Thread(t).start();
		}

	}
}
