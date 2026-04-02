package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBDriver;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class LoadOneDayData {

	private static final String JDBC_PROTOCAL = "jdbc:TSDB://";
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";

	private String host = "127.0.0.1";
	private String configDir = "~/sec/cfg";
	private String user = "root";
	private String password = "taosdata";
	private String jdbcUrl = "";
	private String dbName = "test";

	private Connection conn = null;

	public void MakeJdbcUrl() {
		int port = 0;
		this.jdbcUrl = JDBC_PROTOCAL + host + ":" + port + "/" + dbName + "?user=" + user + "&password=" + password;
		System.out.println(this.jdbcUrl);
	}

	public void SetHost(String host) {
		this.host = host;
	}

	public void setDB(String db) {
		this.dbName = db;
	}

	public void connectdb() {
		Properties info = new Properties();
		info.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, this.configDir);

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

	@SuppressWarnings("finally")
	public ArrayList<String> loadTableNameList(String filePath) {
		ArrayList<String> tableNameList = new ArrayList<String>();

		try {
			FileInputStream fis = new FileInputStream(filePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			while ((line = br.readLine()) != null) {
				tableNameList.add(line);
			}

			fis.close();
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			return tableNameList;
		}
	}

	@SuppressWarnings("finally")
	public ArrayList<String> loadSampleData(String filePath) {
		ArrayList<String> data = new ArrayList<String>();

		try {
			FileInputStream fis = new FileInputStream(filePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			while ((line = br.readLine()) != null) {
				String line1 = new String(line.getBytes(), "UTF-8");
				data.add(line1);
			}

			fis.close();
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			return data;
		}
	}
	
	public void doQuery() {
		Statement stmt = null;

		try {
			stmt = (Statement) conn.createStatement();

			ResultSet rset = stmt.executeQuery("select * from test.tu");
			StringBuilder sb = new StringBuilder();
			
			while(rset.next()) {
				sb.append(rset.getObject(2));
				System.out.println(sb.toString());
//				System.out.print(rset.getString(1) + ", " + rset.getString(2)
//				+ ", " + rset.getObject(3) + ", " + rset.getString(1));
			}

			rset.close();
			stmt.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

	}

	public void insertData(String dir) {
		ArrayList<String> s = this.loadTableNameList(dir + "/devid");
		ArrayList<String> data = this.loadSampleData(dir + "/sample_data");

		Random rand = new Random();

		while (true) {
			long startTime = System.currentTimeMillis();

			Statement stmt = null;

			try {
				stmt = (Statement) conn.createStatement();

				for (String name : s) {
					int r = rand.nextInt(data.size());

					StringBuilder sb = new StringBuilder();
					sb.append("insert into ").append(name).append(" values( ").append(startTime).append(",")
							.append(data.get(r)).append(")");

					String sql = sb.toString();
					stmt.executeUpdate(sql);
				}

			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			long endTime = System.currentTimeMillis();
			System.out.println("insert data completed, elapsed time:" + (endTime - startTime) + " ms");

			try {
				Thread.sleep(27 * 1000L);
				startTime += 27L * 1000;

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void loadLastrow() {
		while (true) {
			long startTime = System.currentTimeMillis();

			Statement stmt = null;
			int num = 0;

			try {
				stmt = (Statement) this.conn.createStatement();

				ResultSet rset = stmt.executeQuery("select last_row(*) from warninginfomt group by tbname");
				while (rset.next()) {
					num += 1;
					String ts = rset.getString(1);
					// do something
				}

				rset.close();
				stmt.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			long endTime = System.currentTimeMillis();
			System.out.println("load rows " + num + " elapsed time:" + (endTime - startTime) + " ms");

			try {
				Thread.sleep(15 * 1000L);
				startTime += 15 * 1000;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
//		if (args.length < 4) {
//			System.out.println("parameters are not sufficient");
//			System.out.println("exe cfg_dir db_name file_dir op_type(load|insert)");
//			System.exit(-1);
//		}

//		System.out.println("cfg:" + args[0]);
//		System.out.println("db:" + args[1]);
//		System.out.println("file dir:" + args[2]);

		LoadOneDayData loader = new LoadOneDayData();
//		loader.setDB(args[1]);
		loader.MakeJdbcUrl();
		loader.connectdb();
		
		loader.doQuery();

//		if (args[3].equals("load")) {
//			System.out.println("start to launch last_row query");
//			loader.loadLastrow();
//		} else if (args[3].equals("insert")) {
//			System.out.println("start to insert data");
//			loader.insertData(args[2]);
//		} else {
//			System.err.println("wrong parameters!");
//			System.out.println("exe cfg_dir db_name file_dir op_type(load|insert)");
//		}
	}
}
