package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class TSDBFanucTest {

	private String host = "192.168.100.128";
	private String configDir = "/etc/taos";
	private int tableCount = 10;
	private int rowsCount = 1000000;
	private String user = "root";
	private String password = "taosdata";
	private String jdbcUrl = "";
	private String dbName = "testdb";
	private String tablePrefix = "testtable";
	private Connection conn = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TSDBFanucTest tester = new TSDBFanucTest();
		tester.ReadArgument(args);

		System.out.println("---------------------------------------------------------------");
		System.out.println("Starting Synchronization Testing...");
		System.out.println("---------------------------------------------------------------");

		tester.MakeJdbcUrl();
		tester.ConnectTbase();
		tester.CreateDbAndTable();
		tester.ExecuteInsert();

		tester.CloseConnection();

		System.out.println("---------------------------------------------------------------");
		System.out.println("Stop Synchronization Testing...");
		System.out.println("---------------------------------------------------------------");

	}

	/*
	 * (non-Java-doc)
	 * 
	 * @see java.lang.Object#Object()
	 */
	public TSDBFanucTest() {
		super();
	}

	public void ReadArgument(String[] args) {
		System.out.println("arguments format : host tables rows config_dir");
		if (args.length >= 1) {
			this.host = args[0];
		}

		if (args.length >= 2) {
			this.tableCount = Integer.parseInt(args[1]);
		}

		if (args.length >= 3) {
			this.rowsCount = Integer.parseInt(args[2]);
		}

		if (args.length >= 4) {
			this.configDir = args[3];
		}
	}

	public void MakeJdbcUrl() {
		String JDBC_PROTOCAL = "jdbc:TSDB://";
		int port = 0;
		String dbName = "";
		this.jdbcUrl = JDBC_PROTOCAL + host + ":" + port + "/" + dbName + "?user=" + user + "&password=" + password;
		System.out.println(this.jdbcUrl);
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

	public void CreateDbAndTable() {
		Statement stmt = null;
		int updateCount = 0;
		try {
			stmt = (Statement) conn.createStatement();

			String sql = "drop database " + dbName;
			updateCount = stmt.executeUpdate(sql);
			if (updateCount == 1) {
				System.out.println(sql + " success");
			} else {
				System.out.println(sql + ", db not exist");
			}

			sql = "create database " + dbName;
			updateCount = stmt.executeUpdate(sql);
			if (updateCount != 1) {
				System.out.println(sql + " falied");
				System.exit(4);
			}
			System.out.println(sql + " success");

			sql = "use " + dbName;
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = "create table mt (ts timestamp, speed int, temp int) tags(tb int)";
			updateCount = stmt.executeUpdate(sql);
			if (updateCount != 1) {
				System.out.println(sql + " failed");
				System.exit(4);
			}

			for (int i = 0; i < this.tableCount; i++) {
				sql = "create table " + this.tablePrefix + i + " using mt tags(" + i + ")";
				updateCount = stmt.executeUpdate(sql);
				if (updateCount != 1) {
					System.out.println(sql + " failed");
					System.exit(4);
				}
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

	public void ExecuteInsert() {
		Statement stmt = null;
		int updateCount = 0;
		long beginTime = (new Date()).getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// ���Է�����޸����ڸ�ʽ
		try {
			long timestamp = 1519833600000L;
			int batch = 3000;
			int printInterval = batch * 10;

			stmt = (Statement) conn.createStatement();

			for (int row = 0; row < this.rowsCount; row += batch) {
				for (int i = 0; i < this.tableCount; ++i) {
					Random rand = new Random();

					StringBuffer buffer = new StringBuffer("insert into " + tablePrefix + i + " values");
					for (int j = 0; j < batch; ++j) {
						buffer.append(
								"(" + (timestamp + row + j) + "," + rand.nextInt(300) + "," + rand.nextInt(100) + ")");
					}
					updateCount = stmt.executeUpdate(buffer.toString());
					if (updateCount != batch) {
						System.out.println(buffer.toString() + " falied");
					}
				}

				if (row % printInterval == 0) {
					long seconds = (new Date()).getTime() - beginTime;
					System.out.println(row * this.tableCount + " rows inserted, spend " + seconds + " ms");
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

		long seconds = (new Date()).getTime() - beginTime;
		System.out.println(
				"all insert finished, " + this.rowsCount * this.tableCount + " rows, spend " + seconds + " ms");
		System.out.println("insert into table success");
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