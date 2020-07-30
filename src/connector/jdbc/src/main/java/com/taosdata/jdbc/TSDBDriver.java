/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver
 * should supply a class that implements the Driver interface
 * 
 * <p>
 * The DriverManager will try to load as many drivers as it can find and then
 * for any given connection request, it will ask each driver in turn to try to
 * connect to the target URL.
 * 
 * <p>
 * It is strongly recommended that each Driver class should be small and stand
 * alone so that the Driver class can be loaded and queried without bringing in
 * vast quantities of supporting code.
 * 
 * <p>
 * When a Driver class is loaded, it should create an instance of itself and
 * register it with the DriverManager. This means that a user can load and
 * register a driver by doing Class.forName("foo.bah.Driver")
 */
public class TSDBDriver implements java.sql.Driver {

	@Deprecated
	private static final String URL_PREFIX1 = "jdbc:TSDB://";

	private static final String URL_PREFIX = "jdbc:TAOS://";

	/**
	 * Key used to retrieve the database value from the properties instance passed
	 * to the driver.
	 */
	public static final String PROPERTY_KEY_DBNAME = "dbname";

	/**
	 * Key used to retrieve the host value from the properties instance passed to
	 * the driver.
	 */
	public static final String PROPERTY_KEY_HOST = "host";
	/**
	 * Key used to retrieve the password value from the properties instance passed
	 * to the driver.
	 */
	public static final String PROPERTY_KEY_PASSWORD = "password";

	/**
	 * Key used to retrieve the port number value from the properties instance
	 * passed to the driver.
	 */
	public static final String PROPERTY_KEY_PORT = "port";

	/**
	 * Key used to retrieve the user value from the properties instance passed to
	 * the driver.
	 */
	public static final String PROPERTY_KEY_USER = "user";

    /**
     * Key for the configuration file directory of TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_CONFIG_DIR = "cfgdir";

    /**
     * Key for the timezone used by the TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_TIME_ZONE = "timezone";

    /**
     * Key for the locale used by the TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_LOCALE = "locale";


    /**
     * Key for the char encoding used by the TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_CHARSET = "charset";

    public static final String PROPERTY_KEY_PROTOCOL = "protocol";

	/**
	 * Index for port coming out of parseHostPortPair().
	 */
	public final static int PORT_NUMBER_INDEX = 1;

	/**
	 * Index for host coming out of parseHostPortPair().
	 */
	public final static int HOST_NAME_INDEX = 0;

	private TSDBDatabaseMetaData dbMetaData = null;

	static {
		try {
			java.sql.DriverManager.registerDriver(new TSDBDriver());
		} catch (SQLException E) {
			throw new RuntimeException(TSDBConstants.WrapErrMsg("can't register tdengine jdbc driver!"));
		}
	}

	public Connection connect(String url, Properties info) throws SQLException {
		if (url == null) {
			throw new SQLException(TSDBConstants.WrapErrMsg("url is not set!"));
		}

		Properties props = null;

		if ((props = parseURL(url, info)) == null) {
			return null;
		}

		try {
			TSDBJNIConnector.init((String) props.get(PROPERTY_KEY_CONFIG_DIR), (String) props.get(PROPERTY_KEY_LOCALE), (String) props.get(PROPERTY_KEY_CHARSET),
					(String) props.get(PROPERTY_KEY_TIME_ZONE));
			Connection newConn = new TSDBConnection(props, this.dbMetaData);
			return newConn;
		} catch (SQLWarning sqlWarning) {
			sqlWarning.printStackTrace();
			Connection newConn = new TSDBConnection(props, this.dbMetaData);
			return newConn;
		} catch (SQLException sqlEx) {
			throw sqlEx;
		} catch (Exception ex) {
			SQLException sqlEx = new SQLException("SQLException:" + ex.toString());
			sqlEx.initCause(ex);
			throw sqlEx;
		}
	}

	/**
	 * Parses hostPortPair in the form of [host][:port] into an array, with the
	 * element of index HOST_NAME_INDEX being the host (or null if not specified),
	 * and the element of index PORT_NUMBER_INDEX being the port (or null if not
	 * specified).
	 * 
	 * @param hostPortPair
	 *            host and port in form of of [host][:port]
	 * 
	 * @return array containing host and port as Strings
	 * 
	 * @throws SQLException
	 *             if a parse error occurs
	 */
	protected static String[] parseHostPortPair(String hostPortPair) throws SQLException {
		String[] splitValues = new String[2];

		int portIndex = hostPortPair.indexOf(":");

		String hostname = null;

		if (portIndex != -1) {
			if ((portIndex + 1) < hostPortPair.length()) {
				String portAsString = hostPortPair.substring(portIndex + 1);
				hostname = hostPortPair.substring(0, portIndex);

				splitValues[HOST_NAME_INDEX] = hostname;

				splitValues[PORT_NUMBER_INDEX] = portAsString;
			} else {
				throw new SQLException(TSDBConstants.WrapErrMsg("port is not proper!"));
			}
		} else {
			splitValues[HOST_NAME_INDEX] = hostPortPair;
			splitValues[PORT_NUMBER_INDEX] = null;
		}

		return splitValues;
	}

	public boolean acceptsURL(String url) throws SQLException {
		return StringUtils.isNotBlank(url) && url.startsWith(URL_PREFIX);
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		if (info == null) {
			info = new Properties();
		}

		if ((url != null) && (url.startsWith(URL_PREFIX) || url.startsWith(URL_PREFIX1))) {
			info = parseURL(url, info);
		}

		DriverPropertyInfo hostProp = new DriverPropertyInfo(PROPERTY_KEY_HOST, info.getProperty(PROPERTY_KEY_HOST));
		hostProp.required = true;

		DriverPropertyInfo portProp = new DriverPropertyInfo(PROPERTY_KEY_PORT,
				info.getProperty(PROPERTY_KEY_PORT, TSDBConstants.DEFAULT_PORT));
		portProp.required = false;

		DriverPropertyInfo dbProp = new DriverPropertyInfo(PROPERTY_KEY_DBNAME, info.getProperty(PROPERTY_KEY_DBNAME));
		dbProp.required = false;
		dbProp.description = "Database name";

		DriverPropertyInfo userProp = new DriverPropertyInfo(PROPERTY_KEY_USER, info.getProperty(PROPERTY_KEY_USER));
		userProp.required = true;

		DriverPropertyInfo passwordProp = new DriverPropertyInfo(PROPERTY_KEY_PASSWORD,
				info.getProperty(PROPERTY_KEY_PASSWORD));
		passwordProp.required = true;

		DriverPropertyInfo[] propertyInfo = new DriverPropertyInfo[5];
		propertyInfo[0] = hostProp;
		propertyInfo[1] = portProp;
		propertyInfo[2] = dbProp;
		propertyInfo[3] = userProp;
		propertyInfo[4] = passwordProp;

		return propertyInfo;
	}

	/**
	 * example: jdbc:TSDB://127.0.0.1:0/db?user=root&password=your_password
	 */

	public Properties parseURL(String url, Properties defaults) throws java.sql.SQLException {
		Properties urlProps = (defaults != null) ? defaults : new Properties();
		if (url == null) {
			return null;
		}

		if (!StringUtils.startsWithIgnoreCase(url, URL_PREFIX) && !StringUtils.startsWithIgnoreCase(url, URL_PREFIX1)) {
			return null;
		}

		String urlForMeta = url;

		String dbProductName = url.substring(url.indexOf(":") + 1);
		dbProductName = dbProductName.substring(0, dbProductName.indexOf(":"));
		int beginningOfSlashes = url.indexOf("//");
		url = url.substring(beginningOfSlashes + 2);

		String host = url.substring(0, url.indexOf(":"));
		url = url.substring(url.indexOf(":") + 1);
		urlProps.setProperty(PROPERTY_KEY_HOST, host);

		String port = url.substring(0, url.indexOf("/"));
		urlProps.setProperty(PROPERTY_KEY_PORT, port);
		url = url.substring(url.indexOf("/") + 1);

		if (url.indexOf("?") != -1) {
			String dbName = url.substring(0, url.indexOf("?"));
			urlProps.setProperty(PROPERTY_KEY_DBNAME, dbName);
			url = url.trim().substring(url.indexOf("?") + 1);
		} else {
			// without user & password so return
			if(!url.trim().isEmpty()) {
				String dbName = url.trim();
				urlProps.setProperty(PROPERTY_KEY_DBNAME, dbName);
			}
			this.dbMetaData = new TSDBDatabaseMetaData(dbProductName, urlForMeta, urlProps.getProperty("user"));
			return urlProps;
		}

		String user = "";

		if (url.indexOf("&") == -1) {
			String[] kvPair = url.trim().split("=");
			if (kvPair.length == 2) {
				setPropertyValue(urlProps, kvPair);	
				return urlProps;
			} 
		}

		String[] queryStrings = url.trim().split("&");		
		for (String queryStr : queryStrings) {
			String[] kvPair = queryStr.trim().split("=");
			if (kvPair.length < 2){
				continue;
			}
			setPropertyValue(urlProps, kvPair);			
		}

		user = urlProps.getProperty(PROPERTY_KEY_USER).toString();
		this.dbMetaData = new TSDBDatabaseMetaData(dbProductName, urlForMeta, user);

		return urlProps;
	}

	public void setPropertyValue(Properties property, String[] keyValuePair) {
		switch (keyValuePair[0].toLowerCase()) {
			case PROPERTY_KEY_USER:
				property.setProperty(PROPERTY_KEY_USER, keyValuePair[1]);					
				break;
			case PROPERTY_KEY_PASSWORD:
				property.setProperty(PROPERTY_KEY_PASSWORD, keyValuePair[1]);
				break;
			case PROPERTY_KEY_TIME_ZONE:
				property.setProperty(PROPERTY_KEY_TIME_ZONE, keyValuePair[1]);
				break;
			case PROPERTY_KEY_LOCALE:
				property.setProperty(PROPERTY_KEY_LOCALE, keyValuePair[1]);
				break;
			case PROPERTY_KEY_CHARSET:
				property.setProperty(PROPERTY_KEY_CHARSET, keyValuePair[1]);
				break;
			case PROPERTY_KEY_CONFIG_DIR:
				property.setProperty(PROPERTY_KEY_CONFIG_DIR, keyValuePair[1]);
				break;
		}
	}


	public int getMajorVersion() {
		return 1;
	}

	public int getMinorVersion() {
		return 1;
	}

	public boolean jdbcCompliant() {
		return false;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

	/**
	 * Returns the host property
	 * 
	 * @param props
	 *            the java.util.Properties instance to retrieve the hostname from.
	 * 
	 * @return the host
	 */
	public String host(Properties props) {
		return props.getProperty(PROPERTY_KEY_HOST, "localhost");
	}

	/**
	 * Returns the port number property
	 * 
	 * @param props
	 *            the properties to get the port number from
	 * 
	 * @return the port number
	 */
	public int port(Properties props) {
		return Integer.parseInt(props.getProperty(PROPERTY_KEY_PORT, TSDBConstants.DEFAULT_PORT));
	}

	/**
	 * Returns the database property from <code>props</code>
	 * 
	 * @param props
	 *            the Properties to look for the database property.
	 * 
	 * @return the database name.
	 */
	public String database(Properties props) {
		return props.getProperty(PROPERTY_KEY_DBNAME);
	}
}
