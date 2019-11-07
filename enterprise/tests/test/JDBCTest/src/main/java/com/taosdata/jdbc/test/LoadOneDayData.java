package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBJNIConnector;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;

public class LoadOneDayData {

	private String host = "127.0.0.1";
	private String configDir = "/etc/taos";
	private String user = "root";
	private String password = "taosdata";
	private String jdbcUrl = "";
	private String dbName = "evi";
	private String tablePrefix = "device";
	private String metricName = "exca_opdata_2018";

	private String startTime = "2018-5-1T0:0:0Z";
	private String endTime = "2018-5-2T0:0:0Z";

	private ArrayList<String> tables = new ArrayList<String>();

	private int numOfThreads = 1;
	private Connection conn = null;

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
		Properties property = new Properties();
		property.
		try {
			TSDBJNIConnector.init(this.configDir, "", "", "");
		} catch (SQLWarning warning) {
			warning.printStackTrace();
		}
//		Properties info = new Properties();
//		info.setProperty(TSDBDriver.CONFIG_DIR_KEY, this.configDir);

		String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
		try {
			Class.forName(TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
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
			stmt.executeUpdate("use " + this.dbName);

			for (int i = 0; i < this.tables.size(); ++i) {
				String sql = "select * from " + this.tables.get(i)
						+ " where receive_time<'%sT0:0:0Z' and receive_time>='%sT0:0:0Z'";
				sql = String.format(sql, this.endTime, this.startTime);

				System.out.println("Execute SQL: " + sql);
				long st = System.currentTimeMillis();

				reSet = stmt.executeQuery(sql);
				if (reSet == null) {
					System.out.println(sql + " failed");
					System.exit(4);
				}

				int numOfRows = 0;
				while (reSet.next()) {
					numOfRows++;
					// resSet.getString(columnIndex);
				}

				System.out.println(sql + " success, fetch rows:" + numOfRows + " elapsed time:"
						+ (System.currentTimeMillis() - st) + "ms");
				reSet.close();
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
	
	public void setTables(ArrayList<String> tables) {
		this.tables = tables;
	}

	public void getAllTables() {
		Statement stmt = null;
		ResultSet reSet = null;
		try {
			stmt = (Statement) conn.createStatement();
			stmt.executeUpdate("use " + this.dbName);
			
			String sql = "select tbname from " + metricName;
			System.out.println("Execute SQL: " + sql);

			reSet = stmt.executeQuery(sql);
			if (reSet == null) {
				System.out.println(sql + " failed");
				return;
			}

			while (reSet.next()) {
				tables.add(reSet.getString(1));
			}

			System.out.println(" fetch " + tables.size() + " tables");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("fetch table failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("fetch table failed");
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

	public ArrayList<ArrayList<String>> calculateQueryTables() {
		int numOfTableOneGroup = (this.tables.size() + this.numOfThreads - 1) / this.numOfThreads;

		ArrayList<ArrayList<String>> groups = new ArrayList<ArrayList<String>>();
		
		int i = 0;
		
		for (int j = 0; j < this.numOfThreads; ++j) {
			ArrayList<String> aList = new ArrayList<String>();
			for (int k=0; k < numOfTableOneGroup && i < this.tables.size(); ++i, ++k) {
				aList.add(this.tables.get(i));
			}
			
			groups.add(aList);
		}
		
		return groups;
	}
	
	void launchThreads() {
		
	}

	public static class Tasks implements Runnable {
		private LoadOneDayData test = new LoadOneDayData();

		public void setHost(String hostIp) {
			test.SetHost(hostIp);
		}

		public void setDB(String db) {
			test.setDB(db);
		}

		public void setTimeRange(String startTime, String endTime) {
			test.setTimeRange(startTime, endTime);
		}
		
		public void setTables(ArrayList<String> tables) {
			test.setTables(tables);
		}

		public void run() {
			test.MakeJdbcUrl();
			test.ConnectTbase();

			Long startTime = System.currentTimeMillis();
			test.ExecuteQuery();

			Long elapsed = System.currentTimeMillis() - startTime;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			System.out.println(sdf.format(System.currentTimeMillis()) +  ", Total elapsed time: " + elapsed + " ms ");
		}
	}

	public static void main(String[] args) {
//		if (args.length < 5) {
//			System.out.println("Usage:\nloaddata [ip address] [db] [numOfThead] [startime] [endtime]");
//			System.exit(-1);
//		}

		args = new String[] {"127.0.0.1", "evi", "1", "2018-11-20", "2018-11-21"};
        
		String IPAddr = args[0];
		String db = args[1];

		int numOfThreads = Integer.parseInt(args[2]);

		String startTime = args[3];
		String endTime = args[4];
		
		LoadOneDayData loader = new LoadOneDayData();
		loader.SetHost(IPAddr);
		loader.setDB(db);
		loader.MakeJdbcUrl();
		loader.ConnectTbase();
		
		loader.getAllTables();
		ArrayList<ArrayList<String>> groups = loader.calculateQueryTables();
		
		for (int i = 0; i < numOfThreads; i++) {
			Tasks t = new Tasks();
			t.setHost(IPAddr);
			t.setDB(db);
			t.setTables(groups.get(i));
			
			t.setTimeRange(startTime, endTime);
			new Thread(t).start();
		}

	}
}
