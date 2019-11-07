package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBDriver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;

public class InsertWholeData {

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

	public void ExecuteQuery(String sql) {
		Statement stmt = null;
		try {
			stmt = (Statement) conn.createStatement();
			int rows = stmt.executeUpdate(sql);
			
			System.out.println("insert rows: " + rows);
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

	private static ArrayList<String> loadData(String filePath) {
		ArrayList<String> lists = new ArrayList<String>();

		BufferedReader bf = null;
		try {
			bf = new BufferedReader(new FileReader(filePath));

			String str;
			while ((str = bf.readLine()) != null) {
				lists.add(str);
			}
			bf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return lists;
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			System.out.println("Usage:\nloaddata [ip address] [db] [tableId] [numOfTables]");
			System.exit(-1);
		}

		String IPAddr = args[0];
		String db = args[1];
		int stable = Integer.parseInt(args[2]);
		int count = Integer.parseInt(args[3]);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long timestamp = 0; // start time
		try {
			timestamp = sdf.parse("2018-05-1 00:00:00").getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		ArrayList<String> data = loadData("/home/taos/0902_excavator_data_f.csv");

		InsertWholeData test = new InsertWholeData();
		test.SetHost(IPAddr);
		test.setDB(db);
		test.MakeJdbcUrl();
		test.ConnectTbase();
		
		System.out.println("start from table:" + stable);

		while (true) {

			Long startTime = System.currentTimeMillis();
			
			for (int i = stable; i < count + stable;) {
				StringBuffer sql = new StringBuffer();
				sql.append("insert into ");
				
				int j = 0;

				while (i < (count+stable) && j < 80) {
					String oneRow = String.format(" device%d values(%d,%s)", i, timestamp, data.get(i % data.size()));
					sql.append(oneRow);
					++i;
					++j;
				}
				
				System.out.println(i);
				test.ExecuteQuery(sql.toString());
			}
			
			Long elapsed = System.currentTimeMillis() - startTime;
			System.out.println("Total elapsed time: " + elapsed + " ms\nSleep: 30 sec.\n");

			try {
				Thread.sleep(25000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			timestamp += 30000;
		}

	}
}
