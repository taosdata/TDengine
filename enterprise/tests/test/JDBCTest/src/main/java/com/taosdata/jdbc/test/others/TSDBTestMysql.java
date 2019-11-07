package com.taosdata.jdbc.test.others;

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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Scanner;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Module: JDBCDriver
 *
 * 
 * @Author Shengli Lin
 * @Date Jun 30, 2017
 */
public class TSDBTestMysql {

	static Scanner input = new Scanner(System.in);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// printUsage();
		TSDBTestMysql mysqlTest = new TSDBTestMysql();
		if (args.length < 1) { // manully test

			Class.forName(MYSQL_JDBC_DRIVER);

			while (true) {
				TSDBCommand cmd = mysqlTest.getAndParseCommand();
				if (cmd.getMajorCmd().equalsIgnoreCase("q") || cmd.getMajorCmd().equalsIgnoreCase("quit")
						|| cmd.getMajorCmd().equalsIgnoreCase("ex") || cmd.getMajorCmd().equalsIgnoreCase("exit")) {
					System.out.println("Goodbye!");
					break;
				} else if (cmd.getMajorCmd().equalsIgnoreCase("h") || cmd.getMajorCmd().equalsIgnoreCase("help")
						|| cmd.getMajorCmd().equalsIgnoreCase("?")) {
					printUsage();
				} else if (cmd.getMajorCmd().equalsIgnoreCase("in") || cmd.getMajorCmd().equalsIgnoreCase("insert")) { // in
																														// Meter1
																														// 100
					String tableName = cmd.getSecondCmd();
					int count = Integer.parseInt(cmd.getThirdCmd());
					mysqlTest.testInsert(tableName, count);
				} else if (cmd.getMajorCmd().equalsIgnoreCase("se") || cmd.getMajorCmd().equalsIgnoreCase("select")) { // se
																														// Meter1
																														// 100
																														// DESC
					String tableName = cmd.getSecondCmd();
					int count = Integer.parseInt(cmd.getThirdCmd());
					mysqlTest.testQuery(tableName, count);
				}
			}
			input.close();
		} else { // automatically test
					// args[0] = tableName
					// args[1] = rowCount

			mysqlTest.testCreateTable(args[0]); // create database demo and table
		
			mysqlTest.testInsert(args[0], Integer.parseInt(args[1]));
			mysqlTest.testQuery(args[0], Integer.parseInt(args[1]));

			mysqlTest.testDropTable(args[0]);
		}

		// TestMysql test = new TestMysql();
		// test.testInsert(2000000);
		// test.testQuery("Meter1",2000000);
	}

	private void testInsert(String tableName, int count) {
		if (tableName == null)
			tableName = "meter1";
		if (count == 0)
			count = 10000;
		Connection con = null;
		Statement stmt = null;

		try {
			con = DriverManager.getConnection(MYSQL_JDBC_URL, "lsl", "lsl7612");
			con.setAutoCommit(true);
			stmt = con.createStatement();
			long startTime = System.currentTimeMillis();
			for (int i = 1; i <= count; i++) {
				// insert into Meter1(temp,high) values(100,1000);
				stmt.executeUpdate("insert into "+tableName+" values(now()," + (i + 1) + ")");
			}
			System.out.println("********MySQL: insert " + count + " rows to take " + (System.currentTimeMillis() - startTime) + " ms");
		} catch (SQLException ex) {
			ex.printStackTrace();

		} finally {
			// TODO: handle finally clause

			try {
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	private void testQuery(String tableName, int count) {
		if (tableName == null)
			tableName = "meter1";
		if (count == 0)
			count = 10000;
		Connection con = null;
		Statement stmt = null;
		ResultSet rest = null;

		try {
			con = DriverManager.getConnection(MYSQL_JDBC_URL);
			con.setAutoCommit(true);
			stmt = con.createStatement();

			long startTime = System.currentTimeMillis();
			rest = stmt.executeQuery("select * from " + tableName);

			int rowIndex = 0;
			for (; rest.next();) {
				if (rowIndex >= count) {
					break;
				}
				rowIndex++;
			}
			System.out.println("********MySQL: query " + count + " rows to take " + (System.currentTimeMillis() - startTime) + " ms");
		} catch (SQLException ex) {
			ex.printStackTrace();

		} finally {
			// TODO: handle finally clause

			try {
				if (rest != null)
					rest.close();
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	private void testCreateTable(String tableName) {
		if (tableName == null)
			tableName = "meter1";
		Connection con = null;
		Statement stmt = null;

		try {
			con = DriverManager.getConnection(MYSQL_JDBC_URL, "lsl", "lsl7612");
			con.setAutoCommit(true);
			stmt = con.createStatement();
			stmt.executeUpdate("create table "+tableName+"(ts timestamp ,height BIGINT)");
		} catch (SQLException ex) {
			ex.printStackTrace();

		} finally {
			// TODO: handle finally clause
			try {
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
	}
	
	
	private void testDropTable(String tableName) {
		if (tableName == null)
			tableName = "meter1";
		
		Connection con = null;
		Statement stmt = null;

		try {
			con = DriverManager.getConnection(MYSQL_JDBC_URL, "lsl", "lsl7612");
			con.setAutoCommit(true);
			stmt = con.createStatement();
		    stmt.executeUpdate("drop table "+tableName);
		} catch (SQLException ex) {
			ex.printStackTrace();

		} finally {
			// TODO: handle finally clause

			try {
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
	

	private TSDBCommand getAndParseCommand() throws IOException {
		TSDBCommand cmd = new TSDBCommand();
		System.out.print("mysqltest> ");

		String gotCommand = input.nextLine();

		if (gotCommand == null || gotCommand.trim().length() == 0) {
			System.out.println("Nothing input? empty is NOT accepted! please input your command!");
			getAndParseCommand();
		}

		String[] cmds = gotCommand.split(" ");

		if (cmds[0].equals("se") || cmds[0].equals("select") || cmds[0].equals("in") || cmds[0].equals("insert")) {
			if (cmds.length < 2) {
				System.out.println("please input correct command!");
				printUsage();
				getAndParseCommand();
			}
		}

		cmd.setMajorCmd(cmds[0]);
		if (cmds.length > 1) {
			cmd.setSeconCmd(cmds[1]);
		}
		if (cmds.length > 2) {
			cmd.setThirdCmd(cmds[2]);
		}
		if (cmds.length > 3) {
			cmd.setFourthCmd(cmds[3]);
		}

		return cmd;
	}

	private static void printUsage() {
		System.out.println("Usage: ");
		System.out.println("");
		System.out.println("   1. q(quit) or ex(exit) will say Goodbye!");
		System.out.println("   2. in Meter1(Table Name) 100 will insert 100 rows into table Meter1");
		System.out.println(
				"   3. se Meter1(Table Name) 100 DESC/ASC(order) will query 100 rows from table Meter1 by order");
		System.out.println("");
	}

	private static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";// "com.mysql.jdbc.Driver";
	private static final String MYSQL_JDBC_URL = "jdbc:mysql://localhost:3306/testdb?useUnicode=true&characterEncoding=utf-8&useSSL=false&user=lsl&password=lsl7612";

}
