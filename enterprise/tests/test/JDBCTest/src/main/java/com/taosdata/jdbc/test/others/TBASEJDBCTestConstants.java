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

/**
 * 
 *
 * @author Shengli Lin
 *  
 * Aug 25, 2017
 */
public abstract class TBASEJDBCTestConstants {
	
	public static final String OUT_INFO_HEAD = "JDBC TEST: ";
	public static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
	
	public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";// "com.mysql.jdbc.Driver";
	public static final String MYSQL_JDBC_URL = "jdbc:mysql://localhost:3306/testdb?useUnicode=true&characterEncoding=utf-8&useSSL=false&user=root&password=taosdata";
	
}
