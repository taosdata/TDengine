package com.taosdata.jdbc.test.others;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**********************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies. 
 *  No part of this file may be reproduced, stored, transmitted, 
 *  disclosed or used in any form or by any means other than as 
 *  expressly provided by the written permission from TAOS Technologies
 *
 * *********************************************************************/

/**
 * 
 *
 * @author Shengli Lin
 * 
 *         Jul 11, 2017
 */
public abstract class TSDBTestUtils {
	
	private static String TSDB_URL = "jdbc:TSDB://192.168.0.1:0/?user=root&password=taosdata";
//	static Connection conn = null;

//	static {
//
//		try {
//			if (conn == null || conn.isClosed()) {
//				Class.forName(TBASEJDBCTestConstants.TSDB_DRIVER);
//				conn = (Connection) DriverManager.getConnection(TSDB_URL);
//			}
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

	public static boolean prepareMySQLDB(String url,String dbName) {
		boolean isSucceed = false;
		Connection conn = null;
		Statement stmt = null;
		int updateCount = 0;
		try {
			Class.forName(TBASEJDBCTestConstants.MYSQL_JDBC_DRIVER);
			if (conn == null || conn.isClosed()) {
				conn = (Connection) DriverManager.getConnection(url);
			}
			stmt = (Statement) conn.createStatement();
			updateCount = stmt.executeUpdate("create database "+ dbName);
			if (updateCount != 1) {
				System.err.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"create database "+ dbName +" failed");
				isSucceed = false;
			}
			else {
				isSucceed = true;
			}
		} catch (ClassNotFoundException e) {
			isSucceed = false;
			e.printStackTrace();
		} catch (SQLException e) {
			isSucceed = false;
			e.printStackTrace();
		} catch (Exception e) {
			isSucceed = false;
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
				    conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return isSucceed;

	}
	
	
	
	public static boolean prepareTBaseDB(String url,String dbName) {
		boolean isSucceed = false;
		Connection conn = null;
		Statement stmt = null;
		int updateCount = 0;
		try {
			Class.forName(TBASEJDBCTestConstants.TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				TSDB_URL = url;
				conn = (Connection) DriverManager.getConnection(TSDB_URL);
			}
			stmt = (Statement) conn.createStatement();
			updateCount = stmt.executeUpdate("create database "+ dbName);
			if (updateCount != 1) {
				System.err.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"create database "+ dbName +" failed");
				isSucceed = false;
			}
			else {
				isSucceed = true;
			}
		} catch (ClassNotFoundException e) {
			isSucceed = false;
			e.printStackTrace();
		} catch (SQLException e) {
			isSucceed = false;
			e.printStackTrace();
		} catch (Exception e) {
			isSucceed = false;
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
				    conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return isSucceed;

	}
	
	public static void dropDB(String url,String dbName) {
		Connection conn = null;
		Statement stmt = null;
		int updateCount = 0;
		try {
			Class.forName(TBASEJDBCTestConstants.TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				TSDB_URL = url;
				conn = (Connection) DriverManager.getConnection(TSDB_URL);
			}
			stmt = (Statement) conn.createStatement();
			updateCount = stmt.executeUpdate("drop database "+ dbName);
			if (updateCount != 1) {
				System.err.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"drop database "+ dbName +" failed");
				return;
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
				    conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}
	
	
	public static void dropMySQLDB(String url,String dbName) {
		Connection conn = null;
		Statement stmt = null;
		int updateCount = 0;
		try {
			Class.forName(TBASEJDBCTestConstants.MYSQL_JDBC_DRIVER);
			if (conn == null || conn.isClosed()) {
				conn = (Connection) DriverManager.getConnection(url);
			}
			stmt = (Statement) conn.createStatement();
			updateCount = stmt.executeUpdate("drop database "+ dbName);
			if (updateCount == 0) {
				System.err.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"drop database "+ dbName +" failed");
				return;
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
				    conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}
	
	public static void prepareDBAndTable(Connection conn, String url,String dbName, String tableName, int tableNumber) {
		if (tableName == null) {
			tableName = "meter1";
		}
		if (tableNumber == 0) {
			tableNumber = 1;
		}

		Statement stmt = null;
		int updateCount = 0;
		try {
			Class.forName(TBASEJDBCTestConstants.TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				TSDB_URL = url;
				//System.out.println(TSDB_URL);
				conn = (Connection) DriverManager.getConnection(TSDB_URL);
			}
			stmt = (Statement) conn.createStatement();
			updateCount = stmt.executeUpdate("create database "+ dbName);
			//System.out.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"create database "+ dbName +" " + (updateCount == 1 ? "succeed!" : "failed"));
			if (updateCount != 1) {
				System.err.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"create database "+ dbName +" failed");
				System.exit(4);
			}
			stmt.executeUpdate("use "+ dbName);
			// updateCount = stmt.executeUpdate(
			// "create table Meter1(ts timestamp, temperature tinyint, pressure
			// smallint, speed int, volume bigint, rpm float, height double)");
			for (int i = 1; i <= tableNumber; i++) {
				updateCount = stmt.executeUpdate("create table " + tableName + i + "(ts timestamp, height bigint)");
				//System.out.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"create table " + tableName +i + (updateCount == 1 ? " succeed!" : " failed"));
			}

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
				    conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void clearDBAndTable(Connection conn, String url, String dbName, String tableName, int tableCount) {
		if (tableName == null)
			tableName = "Meter1";

		Statement stmt = null;
		int updateCount = 0;
		try {
			Class.forName(TBASEJDBCTestConstants.TSDB_DRIVER);
			if (conn == null || conn.isClosed()) {
				TSDB_URL = url;
				conn = (Connection) DriverManager.getConnection(TSDB_URL);
			}
			stmt = (Statement) conn.createStatement();
			//updateCount = stmt.executeUpdate("drop database demo");
			//System.out.println("drop database demo" + (updateCount > 0 ? " succeed!" : " failed"));
			stmt.executeUpdate("use "+dbName);
			for (int i=1;i<=tableCount;i++) {
			    updateCount = stmt.executeUpdate("drop table " + tableName+i);
			    //System.out.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"drop table " + tableName+i + (updateCount == 1 ? " succeed!" : " failed"));
			}
			updateCount = stmt.executeUpdate("drop database "+ dbName);
			//System.out.println(TBASEJDBCTestConstants.OUT_INFO_HEAD+"drop database "+dbName + (updateCount == 1 ? " succeed!" : " failed"));

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
				    conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
