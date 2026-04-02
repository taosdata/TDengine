package com.taosdata.jdbc.test.others;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

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
 *         Jul 28, 2017
 */
public class TSDBTestCassandra {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		Session session = cluster.connect("testks");
		// String cql = "select * from testks.meter1;";
		// ResultSet result = session.execute(cql);
		// System.out.println("result=" + result);
		for (int i = 9000001; i <= 10000000; i++) {
			String cql = "insert into testks.meter1(ts,height) values(" + i + "," + i + ");";
			session.execute(cql);
		}
		
		System.out.println("done.");

	}

}
