package com.taosdata.jdbc.cases;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;

public class App3 {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {

		// 添加数据源1-TDengine,并且作为schema命名为“public”
		String url = "jdbc:TAOS://127.0.0.1:6030/hdb";
		Class.forName("com.taosdata.jdbc.TSDBDriver");
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setUrl(url);
		dataSource.setUsername("root");
		dataSource.setPassword("taosdata");

		// 执行SQL语句，sql语句中表的引用必须用前面设置的schema名称，例如“testdata”就是schema“public“下的表，”datedim“是schema”test“下的表。
		String sql = "select cast(s.updates as date ),t.id,t.cmt from public.testdata t , test.datedim s  "
				+ "where cast(t.uptime as date )=s.updates and t.uptime<'2011-12-01 00:00:00' and t.id>100 limit 100 offset 10 ";
		long startTime = System.currentTimeMillis();
		Connection conn = dataSource.getConnection();
		Statement stmt = conn.createStatement();
//		ResultSet rs = stmt.executeQuery(sql);
		long rowCount = 0;
		String[] types = { "TABLE" };
		ResultSet rs = conn.getMetaData().getTables("hdb", null, null, types);
		while (rs.next()) {
//			if (rowCount < 5) {
			for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
				System.out.print(rs.getMetaData().getColumnName(j + 1) + ": " + rs.getObject(j) + ", ");
			}
			System.out.println();
//			}
			rowCount++;
		}
		rs.close();
		System.out.println("\nGET COLUMN:");
		rs = conn.getMetaData().getColumns("hdb", null, "sdata", null);
		while (rs.next()) {
//			if (rowCount < 5) {
			ResultSetMetaData meta = rs.getMetaData();
			for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
				if (j != 7) {
					System.out.print(rs.getMetaData().getColumnName(j + 1) + ": " + rs.getObject(j) + ", ");
					System.out.println();
				}
			}
			System.out.println();
//			}
			rowCount++;
		}
		long endTime = System.currentTimeMillis();
		System.out.println("execute time:" + (endTime - startTime) + "ms, resultset rows " + rowCount + ", "
				+ rowCount * 1000 / (endTime - startTime) + " rows/sec");
		rs.close();
		stmt.close();
		conn.close();
	}

}
