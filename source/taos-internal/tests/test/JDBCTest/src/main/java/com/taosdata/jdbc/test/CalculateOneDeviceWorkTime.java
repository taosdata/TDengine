package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;

public class CalculateOneDeviceWorkTime {

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
	
	Random ran1 = new Random(290);

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

			int deviceId = ran1.nextInt(10000);
			if (deviceId == 0) {
				deviceId = 1;
			}

			String sql = "select count(*), spread(receive_time) from " + tablePrefix + deviceId + " interval(1d) ";
			sql = String.format(sql, this.endTime, this.startTime);

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			System.out.println(sdf.format(System.currentTimeMillis()) + " Execute SQL: " + sql);

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
	}

	void setQueryMeterRange(int start, int end) {
		this.startIdx = start;
		this.endIdx = end;
	}

	public static class Tasks implements Runnable {
		private CalculateOneDeviceWorkTime test = new CalculateOneDeviceWorkTime();
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
		if (args.length < 3) {
			System.out.println(
					"Usage:\nloaddata [ip address] [db] [numOfTable]");
			System.exit(-1);
		}

		String IPAddr = args[0];
		String db = args[1];

		int numOfThreads = 1;
		int numOfMeters = 1;

		int metersPerThread = numOfMeters / numOfThreads;

//		System.out.println("Meters Per Thread: " + String.valueOf(metersPerThread));

		for (int i = 0; i < numOfThreads; i++) {
			Tasks t = new Tasks();
			t.setQueryRange(i * metersPerThread + 1, (i + 1) * metersPerThread + 1);
			t.setHost(IPAddr);
			t.setDB(db);
            t.setIdleTime(30);
			new Thread(t).start();
		}
	}
}
