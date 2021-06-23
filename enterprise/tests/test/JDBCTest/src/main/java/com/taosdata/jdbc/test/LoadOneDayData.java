package com.taosdata.jdbc.test;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBPreparedStatement;

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

	private String host = "ubuntu";
	private String configDir = "/home/lisa/first/cfg";
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

			ResultSet rset = stmt.executeQuery("select * from test.t2");
			while (rset.next()) {
				System.out.println(rset.getString(1) + ", " + rset.getString(2));
			}

			rset.close();

			stmt.executeUpdate("use test");
			TSDBPreparedStatement s = (TSDBPreparedStatement) conn.prepareStatement("insert into ? values(?, ?)");

			s.setTableName("t2");

			ArrayList<Long> ts = new ArrayList<Long>();
			ts.add(System.currentTimeMillis());
			ts.add(System.currentTimeMillis() + 1);
			ts.add(System.currentTimeMillis() + 3);

			s.setTimestamp(0, ts);

//            ArrayList<Integer> val = new ArrayList<Integer>();
//            val.add(911);
//            val.add(912);
//            s.setInt(1, val);
//            
//            ArrayList<Long> sx = new ArrayList<Long>();
//            sx.add((long) 9);
//            s.setLong(2, sx);

//			ArrayList<String> s1 = new ArrayList<String>();
//			s1.add("aughi");
//			s1.add("abc");
//			s.setString(1, s1, 12);

//            ArrayList<String> s2 = new ArrayList<String>();
//            s2.add("分支");
//            s2.add("分12支");
//            s2.add(null);
//            s.setNString(1, s2, 4);

			s.columnDataAddBatch();
			s.columnDataExecuteBatch();
			s.columnDataCloseBatch();
			stmt.close();

		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.print(e1.getMessage());
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

	public static void main(String[] args) {
		LoadOneDayData loader = new LoadOneDayData();
		loader.MakeJdbcUrl();
		loader.connectdb();

		loader.doQuery();
	}
}
