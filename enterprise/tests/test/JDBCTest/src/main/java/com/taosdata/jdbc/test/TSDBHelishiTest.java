package com.taosdata.jdbc.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TSDBHelishiTest {
	private static final String JDBC_PROTOCAL = "jdbc:TSDB://";

	private String host = "127.0.0.1";
	private String user = "root";
	private String password = "taosdata";
	private int port = 0;
	private String jdbcUrl = "";

	private String databaseName = "db";
	private String metricsName = "mt";
	private String tablePrefix = "t";

	private String fileListName = "./filelist.txt";;
	private int threadNum = 1;
	private int loopCount = 10;
	private int cache = 16384;
	private int ablocks = 8000;
	private int tables = 2000;
	private int loopInterval = 24 * 3600000;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TSDBHelishiTest tester = new TSDBHelishiTest();
		tester.doReadArgument(args);

		System.out.println("---------------------------------------------------------------");
		System.out.println("Start Testing...");
		System.out.println("---------------------------------------------------------------");

		tester.MakeJdbcUrl();

		TSDBHlsReadFile readfile = new TSDBHlsReadFile(tester.fileListName, tester.tablePrefix);
		readfile.run();

		TSDBHlsCreateSchema createSchema = new TSDBHlsCreateSchema(tester.jdbcUrl, tester.databaseName,
				tester.metricsName, readfile, tester.cache, tester.ablocks, tester.tables);
		createSchema.run();

		ExecutorService executorService = Executors.newFixedThreadPool(tester.threadNum);
		int tablePerThread = readfile.tables.size() / tester.threadNum;
		for (int i = 0; i < tester.threadNum; i++) {
			int tableBegin = i * tablePerThread;
			int tableEnd = (i + 1) * tablePerThread;
			if (i == tester.threadNum - 1) {
				tableEnd = readfile.tables.size();
			}
			executorService.execute(new TSDBHlsInsertData(i, tester.jdbcUrl, tester.databaseName, readfile, tableBegin,
					tableEnd, tester.loopCount, tester.loopInterval));
		}

		executorService.shutdown();
		while (!executorService.isTerminated()) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}

		System.out.println("---------------------------------------------------------------");
		System.out.println("All insert thread finished...");
		System.out.println("---------------------------------------------------------------");

		TSDBHlsQueryData queryData = new TSDBHlsQueryData(tester.jdbcUrl, tester.databaseName, tester.metricsName);
		queryData.run();

		System.out.println("---------------------------------------------------------------");
		System.out.println("Stop Testing...");
		System.out.println("---------------------------------------------------------------");
	}

	private void doReadArgument(String[] args) {
		System.out.println("arguments format : filelist.txt threads loopCount cache ablocks tables host");

		if (args.length >= 1) {
			this.fileListName = args[0];
		}

		if (args.length >= 2) {
			this.threadNum = Integer.parseInt(args[1]);
		}

		if (args.length >= 3) {
			this.loopCount = Integer.parseInt(args[2]);
		}

		if (args.length >= 4) {
			this.cache = Integer.parseInt(args[3]);
		}

		if (args.length >= 5) {
			this.ablocks = Integer.parseInt(args[4]);
		}

		if (args.length >= 6) {
			this.tables = Integer.parseInt(args[5]);
		}

		if (args.length >= 7) {
			this.host = args[6];
		}

		System.out.printf("arguments threads:%d filelist:%s loop:%d cache:%d ablocks:%d tables:%d host:%s \n",
				this.threadNum, this.fileListName, this.loopCount, this.cache, this.ablocks, this.tables, this.host);
	}

	private void MakeJdbcUrl() {
		// jdbc:TSDB://192.168.0.1:0/dbname?user=root&password=taosdata
		this.jdbcUrl = String.format("%s%s:%d/%s?user=%s&password=%s", JDBC_PROTOCAL, this.host, this.port, "",
				this.user, this.password);
		System.out.println(this.jdbcUrl);
	}
}

class TSDBHlsRow {
	long serverTime;
	long sourceTime;
	StringBuilder data;

	public TSDBHlsRow(long serverTime, long sourceTime, StringBuilder data) {
		this.serverTime = serverTime;
		this.sourceTime = sourceTime;
		this.data = data;
	}
}

class TSDBHlsTable {
	public String tbName;
	public int tbId;
	public int mtType;
	public ArrayList<TSDBHlsRow> dataList = new ArrayList<TSDBHlsRow>();

	public TSDBHlsTable(String tbName, int tbId, int mtType, TSDBHlsRow data) {
		this.tbName = tbName;
		this.tbId = tbId;
		this.mtType = mtType;
		this.dataList.add(data);
	}
}

class TSDBHlsRawData {
	private static final long magic = 116444736000000000l;
	public String nodeId;
	public String subIndex;
	public long nodeValueServerTime;
	public long nodeValueSourceTime;
	public String nodeValue;
	public String nodeValueQuality;
	public String nodeValueType;
	public String udpArrayValue;

	public TSDBHlsRawData(String sample) {
		String[] sampleInfos = sample.split(" ");
		this.nodeId = sampleInfos[4].replace("(", "").replace(",", "");
		this.subIndex = sampleInfos[5].replace(",", "");

		long nodeValueServerTime = Long.valueOf(sampleInfos[6].replace(",", ""));
		this.nodeValueServerTime = BigDecimal.valueOf(nodeValueServerTime - magic)
				.divide(BigDecimal.valueOf(10000l), BigDecimal.ROUND_HALF_UP).longValue();

		long nodeValueSourceTime = Long.valueOf(sampleInfos[7].replace(",", ""));
		this.nodeValueSourceTime = BigDecimal.valueOf(nodeValueSourceTime - magic)
				.divide(BigDecimal.valueOf(10000l), BigDecimal.ROUND_HALF_UP).longValue();

		this.nodeValue = sampleInfos[8].replace(",", "").replaceAll("'", "");
		this.nodeValueQuality = sampleInfos[9].replace(",", "");
		this.nodeValueType = sampleInfos[10].replace(",", "");
		this.udpArrayValue = sampleInfos[11].replace(");", "");
	}
}

class TSDBHlsReadFile {
	private String fileListName;
	private String tablePrefix;
	private ArrayList<String> fileNames = new ArrayList<String>();
	public ArrayList<TSDBHlsTable> tables = new ArrayList<TSDBHlsTable>();

	public TSDBHlsReadFile(String fileListName, String tablePrefix) {
		this.fileListName = fileListName;
		this.tablePrefix = tablePrefix;
	}

	public void run() {
		this.ReadFileList();
		this.ReadDataFiles();
	}

	private void ReadFileList() {
		try {
			File file = new File(this.fileListName);
			if (file.exists()) {
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String filename = "";
				while ((filename = reader.readLine()) != null) {
					this.fileNames.add(filename);
				}
				System.out.printf("file %s has been read\n", this.fileListName);
				reader.close();
			} else {
				throw new IOException("File not exist.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.printf("filelist:%s read finished, total %d files\n", this.fileListName, this.fileNames.size());
	}

	private void ReadDataFiles() {
		int start = (int) System.currentTimeMillis();
		TSDBHlsTable[] tmpTables = new TSDBHlsTable[60000];

		for (int i = 0; i < this.fileNames.size(); i++) {
			String filePath = this.fileNames.get(i);
			File file = new File(filePath);
			
			try {
				if (file.exists()) {
					BufferedReader reader = new BufferedReader(new FileReader(file));
					String sample = "";

					while ((sample = reader.readLine()) != null) {
						TSDBHlsRawData record = new TSDBHlsRawData(sample);
						int tbId = Integer.valueOf(record.nodeId);
						StringBuilder rowData = new StringBuilder(record.subIndex).append(",")
								.append(record.nodeValueQuality).append(",").append(record.nodeValue).append(",")
								.append(record.udpArrayValue).append(") ");

						if (tmpTables[tbId] == null) {
							String tbName = this.tablePrefix + record.nodeId;
							TSDBHlsRow row = new TSDBHlsRow(record.nodeValueServerTime, record.nodeValueSourceTime,
									rowData);
							int mtType = ("7".equals(record.nodeValueType.trim())) ? 7 : 11;
							TSDBHlsTable hlsTable = new TSDBHlsTable(tbName, tbId, mtType, row);
							tmpTables[tbId] = hlsTable;
						} else {
							int last = tmpTables[tbId].dataList.size() - 1;
							if (record.nodeValueServerTime > tmpTables[tbId].dataList.get(last).serverTime) {
								TSDBHlsRow row = new TSDBHlsRow(record.nodeValueServerTime, record.nodeValueSourceTime,
										rowData);
								tmpTables[tbId].dataList.add(row);
							}
							
						}
					}
					System.out.printf("file %s has been read\n", filePath);
					reader.close();
				} else {
					throw new IOException("File not exist.");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		int end = (int) System.currentTimeMillis();
		System.out.printf("total %d files read, time spend %d seconds\n", this.fileNames.size(), (end - start) / 100);
		
		tables.ensureCapacity(60000);
		for (int i = 0; i < 60000; ++i) {
			if (tmpTables[i] != null) {
				tables.add(tmpTables[i]);
			}
		}
	}
}

class TSDBHlsCreateSchema {
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private String jdbcUrl;
	private String databaseName;
	private String metricsName;
	private TSDBHlsReadFile file;
	private int cache;
	private int ablocks;
	private int tables;
	private Connection conn = null;

	public TSDBHlsCreateSchema(String jdbcUrl, String databaseName, String metricsName, TSDBHlsReadFile file, int cache,
			int ablocks, int tables) {
		this.jdbcUrl = jdbcUrl;
		this.databaseName = databaseName;
		this.metricsName = metricsName;
		this.file = file;
		this.cache = cache;
		this.ablocks = ablocks;
		this.tables = tables;
	}

	public void run() {
		this.ConnectToTaosd();
		this.CreateSchema();
		this.CloseConnection();
	}

	private void ConnectToTaosd() {
		try {
			Class.forName(TSDB_DRIVER);
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
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
		System.out.printf("create schema, get connection from %s success\n", this.jdbcUrl);
	}

	private void CreateSchema() {
		Statement stmt = null;
		int start = (int) System.currentTimeMillis();
		try {
			stmt = (Statement) this.conn.createStatement();

			String sql = String.format("create database if not exists %s cache %d ablocks %d tblocks 500 tables %d",
					this.databaseName, this.cache, this.ablocks, this.tables);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format("use %s", this.databaseName);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format(
					"create table if not exists %s%d (ts timestamp, sourceTime timestamp, subIndex smallint, nodeValueQuality smallint, nodeValue double, udpArrayValue binary(4)) tags(nodeId int)",
					this.metricsName, 11);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			sql = String.format(
					"create table if not exists %s%d (ts timestamp, sourceTime timestamp, subIndex smallint, nodeValueQuality smallint, nodeValue int, udpArrayValue binary(4)) tags(nodeId int)",
					this.metricsName, 7);
			stmt.executeUpdate(sql);
			System.out.println(sql + " success");

			int tablesCount = file.tables.size();
			for (int i = 0; i < tablesCount; i++) {
				TSDBHlsTable table = file.tables.get(i);
				sql = String.format("create table if not exists %s using %s%d tags(%d)", table.tbName, this.metricsName,
						table.mtType, table.tbId);
				stmt.executeUpdate(sql);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("create schema failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("create schema failed");
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
		System.out.printf("Total %d tables created, time spend %d seconds.\n", this.file.tables.size(),
				(end - start) / 1000);
	}

	private void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class TSDBHlsInsertData implements Runnable {
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private int threadIndex;
	private String jdbcUrl;
	private String databaseName;
	private TSDBHlsReadFile file;
	private int tablesBegin;
	private int tablesEnd;
	private int loopCount;
	private int loopInterval;
	private int batchSize = 500;
	private int retryTimes = 5;

	private Connection conn = null;
	private long rowsInserted = 0;

	public TSDBHlsInsertData(int threadIndex, String jdbcUrl, String databaseName, TSDBHlsReadFile file,
			int tablesBegin, int tablesEnd, int loopCount, int loopInterval) {
		this.threadIndex = threadIndex;
		this.jdbcUrl = jdbcUrl;
		this.databaseName = databaseName;
		this.file = file;
		this.tablesBegin = tablesBegin;
		this.tablesEnd = tablesEnd;
		this.loopCount = loopCount;
		this.loopInterval = loopInterval;
	}

	@Override
	public void run() {
		this.ConnectToTaosd();
		this.UseDb();
		this.ExecuteInsert();
		this.CloseConnection();
	}

	private void ConnectToTaosd() {
		try {
			Class.forName(TSDB_DRIVER);
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
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
		System.out.printf("Thread:%d, get connection from %s success\n", this.threadIndex, this.jdbcUrl);
	}

	private void UseDb() {
		Statement stmt = null;
		try {
			stmt = (Statement) this.conn.createStatement();
			String sql = String.format("use %s", this.databaseName);
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("use db failed");
			System.exit(4);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("use db failed");
			System.exit(4);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void ExecuteInsert() {
		int start = (int) System.currentTimeMillis();
		long databaseTime = 0;

		for (int loop = 0; loop < loopCount; loop++) {

			int end = (int) System.currentTimeMillis();
			System.out.printf("Thread:%d, loop:%d:%d, %d rows inserted, time spend %d seconds.\n", this.threadIndex,
					loop, loopCount, this.rowsInserted, (end - start) / 1000);

			long timeIncrement = this.loopInterval * loop;
			
			StringBuffer buffer = new StringBuffer();
			buffer.append("insert into ");
			int batchCount = 0;
			
			for (int tableIndex = this.tablesBegin; tableIndex < this.tablesEnd; ++tableIndex) {
				TSDBHlsTable table = file.tables.get(tableIndex);
				buffer.append(table.tbName).append(" values");

				int rowSize = table.dataList.size();
				for (int rowIndex = 0; rowIndex < rowSize; ++rowIndex) {
					TSDBHlsRow row = table.dataList.get(rowIndex);
					buffer.append("(").append(row.serverTime + timeIncrement).append(", ")
							.append(row.sourceTime + timeIncrement).append(", ").append(row.data);
					batchCount ++;

					if (batchCount == this.batchSize) {
						int tmp1 = (int) System.currentTimeMillis();
						this.ExecuteSql(buffer.toString());
						databaseTime += ((int) System.currentTimeMillis() - tmp1);
						buffer.delete(0, buffer.length());
						buffer.append("insert into ");
						if (rowIndex != rowSize -1) {
							buffer.append(table.tbName).append(" values");
						}
						batchCount = 0;
					}
				}
			}
			
			if (batchCount != 0) {
				int tmp1 = (int) System.currentTimeMillis();
				this.ExecuteSql(buffer.toString());
				databaseTime += ((int) System.currentTimeMillis() - tmp1);
			}
		}
		
		int end = (int) System.currentTimeMillis();
		System.out.printf(
				"Thread:%d, total %d rows inserted, %d tables, database time %d seconds, total time spend %d seconds.\n",
				this.threadIndex, this.rowsInserted, (this.tablesEnd - this.tablesBegin), databaseTime / 1000,
				(end - start) / 1000);
	}
	
	private void ExecuteSql(String sql) {
		for (int i = 0; i <this.retryTimes; ++i) {
			Statement stmt = null;
			try {
				stmt = (Statement) conn.createStatement();
				int affectRows = stmt.executeUpdate(sql);
				this.rowsInserted += affectRows;
				break;
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.printf("sql:%s execute failed\n", sql);
				System.exit(4);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.printf("sql:%s execute failed\n", sql);
				System.exit(4);
			} finally {
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}
	}

	private void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class TSDBHlsQueryData {
	private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	private String jdbcUrl;
	private String databaseName;
	private String metricsName;
	private Connection conn = null;

	public TSDBHlsQueryData(String jdbcUrl, String databaseName, String metricsName) {
		this.jdbcUrl = jdbcUrl;
		this.databaseName = databaseName;
		this.metricsName = metricsName;
	}

	public void run() {
		this.ConnectToTaosd();
		this.ExecuteQuery(7);
		this.ExecuteQuery(11);
		this.CloseConnection();
	}

	private void ConnectToTaosd() {
		try {
			Class.forName(TSDB_DRIVER);
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
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
		System.out.printf("query data, get connection from %s success\n", this.jdbcUrl);
	}

	private void ExecuteQuery(int mtType) {
		Statement stmt = null;
		ResultSet resSet = null;
		try {
			stmt = (Statement) conn.createStatement();
			String sql = String.format("select count(*) as total%d from %s.%s%d", mtType, this.databaseName,
					this.metricsName, mtType);

			resSet = stmt.executeQuery(sql);
			if (resSet == null) {
				System.out.println(sql + " failed");
				System.exit(4);
			}

			ResultSetMetaData metaData = resSet.getMetaData();
			for (int column = 0; column < metaData.getColumnCount(); ++column) {
				System.out.println(
						column + ", " + metaData.getColumnName(column) + ", " + metaData.getColumnType(column) + ", "
								+ metaData.getColumnTypeName(column) + ", " + metaData.getColumnDisplaySize(column));
			}
			int queryCount = 0;
			while (resSet.next()) {
				StringBuffer strBuff = new StringBuffer();
				for (int col = 0; col < metaData.getColumnCount(); col++) {
					strBuff.append(metaData.getColumnName(col)).append(" = ").append(resSet.getObject(col)).append(" ");
				}
				System.out.println(strBuff.toString());
				queryCount++;
			}

			System.out.println(sql + " success, rows in resultset:" + queryCount);
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

	private void CloseConnection() {
		try {
			if (this.conn != null)
				this.conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
