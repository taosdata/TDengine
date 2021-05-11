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
	private String configDir = "~/first/cfg";
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

			ResultSet rset = stmt.executeQuery("select * from test.tm1");
			while(rset.next()) {
				System.out.println(rset.getString(1) + ", " + rset.getString(2));
			}

			//ts timestamp, a1 int, a2 smallint, a3 bigint, a4 binary(12), a5 nchar(12), a6 tinyint
			TSDBPreparedStatement s = (TSDBPreparedStatement) conn.prepareStatement("insert into ? values(?, ?, ?, ?, ?, ?, ?)");
			s.setTableName("t5");
			
			ArrayList<Long> t1 = new ArrayList<Long>();
			t1.add(System.currentTimeMillis());
			t1.add(System.currentTimeMillis() + 1);
			t1.add(System.currentTimeMillis() + 2);
			s.setTimestamp(0, t1);
			
			ArrayList<Integer> b2 = new ArrayList<Integer>();
			b2.add(1);
			b2.add(2);
			b2.add(null);
			s.setInt(1, b2);
			
			ArrayList<Short> b3 = new ArrayList<Short>();
			b3.add((short) 1);
			b3.add((short) 2);
			b3.add(null);
			s.setShort(2, b3);
			
			ArrayList<Long> b4 = new ArrayList<Long>();
			b4.add(1L);
			b4.add(2L);
			b4.add(null);
			s.setLong(3, b4);
			
			ArrayList<String> b5 = new ArrayList<String>();
			b5.add(new String("abc"));
			b5.add(new String("def"));
			b5.add(null);
			s.setString(4, b5, 12);
			
			ArrayList<String> b6 = new ArrayList<String>();
			b6.add("zzz");
			b6.add("zzzz");
			b6.add(null);
			s.setNString(5, b6, 12);
			
			ArrayList<Byte> b7 = new ArrayList<Byte>();
			b7.add((byte) 1);
			b7.add((byte) 2);
			b7.add(null);
			s.setByte(6, b7);
			
			s.columnDataAddBatch();
			s.columnDataExecuteBatch();
			s.columnDataCloseBatch();
			
			rset.close();
			stmt.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

	}

	public static void main(String[] args) {
//		if (args.length < 4) {
//			System.out.println("parameters are not sufficient");
//			System.out.println("exe cfg_dir db_name file_dir op_type(load|insert)");
//			System.exit(-1);
//		}

		LoadOneDayData loader = new LoadOneDayData();
		loader.MakeJdbcUrl();
		loader.connectdb();
		
		loader.doQuery();
	}
}
