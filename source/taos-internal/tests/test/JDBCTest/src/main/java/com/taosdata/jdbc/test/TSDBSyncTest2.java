package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;

//java -jar synctest2.jar 192.168.3.5 10 1000000 ~/work/sim/ubuntu/cfg

public class TSDBSyncTest2 {
	private String host = "ubuntu";
	private String configDir = "/etc/taos";
	private int tableCount = 10;
	private int rowsCount = 1000000;
	private String user = "root";
	private String password = "taosdata";
	private String jdbcUrl = "";
	private String dbName = "syncdb2";
	private String tablePrefix = "table";
	private Connection conn = null;

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

			String sql = "drop database if exists " + dbName;
			updateCount = stmt.executeUpdate(sql);
			if (updateCount == 1) {
				System.out.println(sql + " success");
			} else {
				System.out.println(sql + ", db not exist");
			}

			sql = "create database " + dbName;
			updateCount = stmt.executeUpdate(sql);
			if (updateCount != 0) {
				System.out.println(sql + " failed");
				System.exit(4);
			}
			
			System.out.println(sql + " success");

			sql = "use " + dbName;
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = "create table m1(timeid TIMESTAMP, field1 BINARY(50), field2 BINARY(20), field3 BINARY(16), field4 INT, field5 INT, field6 INT) tags(type int)";
			updateCount = stmt.executeUpdate(sql);
			if (updateCount != 0) {
				System.out.println(sql + " failed");
				System.exit(4);
			}
			System.out.println(sql + " success");

			for (int i = 0; i < this.tableCount; i++) {
				sql = "create table " + this.tablePrefix + i + " using m1 tags(" + i + ")";
				updateCount = stmt.executeUpdate(sql);
				if (updateCount != 0) {
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

		try {
			long timestamp = 1519833600000L;
			stmt = (Statement) conn.createStatement();
			for (int row = 0; row < this.rowsCount; ++row) {
				long curtime = timestamp + row;
				int tryTimes = 0;
				for (int i = 0; i < this.tableCount; ++i) {
					StringBuffer sb = new StringBuffer();
					sb.append("INSERT INTO ").append(tablePrefix).append(i).append(" VALUES (").append(curtime)
							.append(", \"").append(row).append("\", \"").append(row).append("\", \"").append(row)
							.append("\", ").append(row).append(", ").append(row).append(", ").append(row).append(")");

					String sql = sb.toString();
					// System.out.println(sql);
					updateCount = stmt.executeUpdate(sql);

					if (updateCount != 1) {
						if (tryTimes <= 3) {
							tryTimes++;
							--i;
						} else {
							System.out.println(sql + " falied");
						}
					}
				}

				if (++row % 10000 == 0) {
					System.out.println(row + " rows inserted");
				}
			}
			
			System.out.println("all data inserted, total rows:" +  this.tableCount * this.rowsCount);

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
		System.out.println("insert into table success");
	}

	public void ExecuteQuery() {
		Statement stmt = null;
		ResultSet resSet = null;
		try {
			stmt = (Statement) conn.createStatement();
			for (int i = 0; i < this.tableCount; ++i) {
				String sql = "select * from " + tablePrefix + i;
				System.out.println(sql);

				resSet = stmt.executeQuery(sql);
				if (resSet == null) {
					System.out.println(sql + " falied");
					System.exit(4);
				}

				ResultSetMetaData metaData = resSet.getMetaData();
				int queryCount = 0;
				while (resSet.next()) {
					// StringBuffer strBuff = new StringBuffer();
					// for (int col = 0; col < metaData.getColumnCount(); col++) {
					// strBuff.append(metaData.getColumnName(col) + "="
					// + TSDBResultSetUtils.getValueFromResultSet(resSet, col, metaData) + " ");
					// }
					// System.out.println(strBuff);
					queryCount++;
				}
				if (queryCount != this.rowsCount) {
					System.out.println(sql + " failed, querycount:" + queryCount + ", insertCount:" + this.rowsCount);
				} else {
					System.out.println(sql + " success, querycount:" + queryCount);
				}
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

	public void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		TSDBSyncTest2 tester = new TSDBSyncTest2();
		tester.ReadArgument(args);

		System.out.println("---------------------------------------------------------------");
		System.out.println("Starting Synchronization Testing...");
		System.out.println("---------------------------------------------------------------");

		tester.MakeJdbcUrl();
		tester.ConnectTbase();
		tester.CreateDbAndTable();
		tester.ExecuteInsert();
		tester.ExecuteQuery();
		tester.CloseConnection();

		System.out.println("---------------------------------------------------------------");
		System.out.println("Stop Synchronization Testing...");
		System.out.println("---------------------------------------------------------------");
	}
}
