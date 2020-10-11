package com.tdengine.jdbc;


import com.common.ReadProperties;
import com.taosdata.jdbc.TSDBDriver;
import com.tdengine.rest.JsonPost;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


/**
 * 功能描述：读取解析 txt 文件
 *
 * @Author: fang xinliang
 * @Date: 2020/10/2 12:32
 */
public class RainStation {
	private final static Log logger = LogFactory.getLog(RainStation.class);
	// 10年，2000 站点，雨量数据文件数量
	static List<Connection>  connList = new ArrayList(5);


	public static void txtSave2DB(String[] paramsArray) throws IOException {

			String ts = paramsArray[2];
			String tName = paramsArray[1];
			String tCode = paramsArray[0];
			String tType = tCode.indexOf("V") > -1 ? "2" : "1";
			String rainFall = paramsArray[3];
		    String sql = " insert into rainstation.S_" + tCode + " using rainstation.monitoring tags('" + tCode + "','" + tName + "','" + tType + "' )  (ts,rainfall) values ('" + ts + "','" + rainFall + "') ";

            String ServerType = ReadProperties.read("STYPE");
            if("db".equals(ServerType)){
				executeUpdate(sql);
			}
            else{
				JsonPost.HttpPostWithJson(sql);
			}
	}
	public static  Connection getConn() throws Exception {
		Class.forName("com.taosdata.jdbc.TSDBDriver");
		Properties connProps = new Properties();
		final String jdbcUrlwithoutIp = ReadProperties.read("PROPERTY_JDBC_URL"); //jdbc:TAOS://127.0.0.1:6030/";
		connProps.setProperty(TSDBDriver.PROPERTY_KEY_USER, ReadProperties.read("PROPERTY_KEY_USER")); //"root"
		connProps.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, ReadProperties.read("PROPERTY_KEY_PASSWORD")); //"taosdata"
		connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
		connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
		connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
		Connection conn = DriverManager.getConnection(jdbcUrlwithoutIp, connProps);
		return conn;
	}
	public static void executeUpdate( String sql)  {
		try {
			//Random ra =new Random();
			//int index = ra.nextInt(5);
			Connection conn = getConn();
			Statement stat = conn.createStatement();
			boolean execute = stat.execute(sql);
			logger.info("execute[" + sql + "] ===> " + execute);
			stat.close();
			conn.close();
		}
		catch (SQLException ex){
			logger.error(ex.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws Exception {
		//
		//System.out.println(filesList.size());
		//
        logger.info("开始："+new Date().toLocaleString());
		String[] paramsArray = {"V00003","其他站点","2020-10-08 23:19:20.126","10"};
		txtSave2DB(  paramsArray);

		logger.info("结束："+new Date().toLocaleString());

	}
}
