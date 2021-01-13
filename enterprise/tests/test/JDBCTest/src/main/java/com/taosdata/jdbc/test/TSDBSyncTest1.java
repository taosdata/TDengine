package com.taosdata.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TSDBSyncTest1 {
	private String host = "ubuntu";
	private String configDir = "/etc/taos";
	private int tableCount = 1;
	private int rowsCount = 1000;
	private String user = "root";
	private String password = "taosdata";
	private String jdbcUrl = "";
	private String dbName = "syncdb1";
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

		String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
		try {
			Class.forName(TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				Properties a = System.getProperties();
				String b = a.getProperty("sun.jnu.encoding");
				String c = a.getProperty("file.encoding");
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

	public void CreateDbAndTable() {
		Statement stmt = null;
		int code = 0;
		try {
			stmt = (Statement) conn.createStatement();

			String sql = "drop database if exists " + dbName;
			code = stmt.executeUpdate(sql);
			if (code != 0) {
				System.out.println(sql + " failed");
			}

			sql = "create database " + dbName;
			code = stmt.executeUpdate(sql);
			if (code != 0) {
				System.out.println(sql + " falied");
				System.exit(4);
			}
			System.out.println(sql + " success");

			sql = "use " + dbName;
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			for (int i = 0; i < this.tableCount; i++) {
				//sql = "create table " + this.tablePrefix + i + "(ts timestamp, f1 bool, f2 tinyint, f3 smallint, f4 int, f5 bigint, f6 bigint, f7 float, f8 double, f9 binary(20), f10 nchar(20))";
				sql = "create table " + this.tablePrefix + i + "(ts timestamp, f1 int)";
				
				code = stmt.executeUpdate(sql);
				if (code != 0) {
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
		int affectRows = 0;
		int start = (int) System.currentTimeMillis();
		try {
			long timestamp = 1519833600000L;
			stmt = (Statement) conn.createStatement();
			for (int row = 0; row < this.rowsCount; ++row) {
				long curtime = timestamp + row;
				for (int i = 0; i < this.tableCount; ++i) {
					StringBuffer buffer = new StringBuffer("insert into ");
					buffer.append(tablePrefix).append(i).append(" values(").append(curtime)
					.append(", ").append(row % 2)
					//.append(", ").append(row % 100)
					//.append(", ").append(row % 30000)
					//.append(", ").append(row)
					//.append(", ").append(row)
					//.append(", ").append("NULL")
					//.append(", ").append(row)
					//.append(", ").append(row)
					//.append(", '").append(row).append("'")
					//.append(", '").append("ABC").append("'")
					.append(")");
					
					affectRows = stmt.executeUpdate(buffer.toString());
					if (affectRows != 1) {
						System.out.println(buffer.toString() + " falied");
					}
				}

				if (row % 10000 == 0) {
					System.out.println(row + " rows inserted");
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
		System.out.println("insert into table success, time spend " + (end-start)/1000 + " seconds.");
	}

	public void ExecuteQuery() {
		Statement stmt = null;
		ResultSet resSet = null;
		try {
			stmt = (Statement) conn.createStatement();
			for (int i = 0; i < this.tableCount; ++i) {
				String sql = "select * from " + tablePrefix + i;
				//String sql = "select * from db.tb";
				
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
						strBuff.append(metaData.getColumnName(col)).append("=").append(resSet.getObject(col)).append(" ");
					}
					System.out.println(strBuff.toString());
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
		TSDBSyncTest1 tester = new TSDBSyncTest1();
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
