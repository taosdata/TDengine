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

import java.sql.*;
import java.util.*;
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
public class TSDBDriver extends AbstractTaosDriver {

    @Deprecated
    private static final String URL_PREFIX1 = "jdbc:TSDB://";

    private static final String URL_PREFIX = "jdbc:TAOS://";

    /**
     * Key used to retrieve the host value from the properties instance passed to
     * the driver.
     */
    public static final String PROPERTY_KEY_HOST = "host";
    /**
     * Key used to retrieve the port number value from the properties instance
     * passed to the driver.
     */
    public static final String PROPERTY_KEY_PORT = "port";
    /**
     * Key used to retrieve the database value from the properties instance passed
     * to the driver.
     */
    public static final String PROPERTY_KEY_DBNAME = "dbname";
    /**
     * Key used to retrieve the user value from the properties instance passed to
     * the driver.
     */
    public static final String PROPERTY_KEY_USER = "user";
    /**
     * Key used to retrieve the password value from the properties instance passed
     * to the driver.
     */
    public static final String PROPERTY_KEY_PASSWORD = "password";
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

    private TSDBDatabaseMetaData dbMetaData = null;

    static {
        try {
            java.sql.DriverManager.registerDriver(new TSDBDriver());
        } catch (SQLException E) {
            throw new RuntimeException(TSDBConstants.WrapErrMsg("can't register tdengine jdbc driver!"));
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null)
            throw new SQLException(TSDBConstants.WrapErrMsg("url is not set!"));

        if (!acceptsURL(url))
            return null;

        Properties props = null;
        if ((props = parseURL(url, info)) == null) {
            return null;
        }
        //load taos.cfg start
        loadTaosConfig(info);

        try {
            TSDBJNIConnector.init((String) props.get(PROPERTY_KEY_CONFIG_DIR), (String) props.get(PROPERTY_KEY_LOCALE),
                    (String) props.get(PROPERTY_KEY_CHARSET), (String) props.get(PROPERTY_KEY_TIME_ZONE));
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
     * @param url the URL of the database
     * @return <code>true</code> if this driver understands the given URL;
     * <code>false</code> otherwise
     * @throws SQLException if a database access error occurs or the url is {@code null}
     */
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null)
            throw new SQLException(TSDBConstants.WrapErrMsg("url is null"));
        return (url != null && url.length() > 0 && url.trim().length() > 0) && (url.startsWith(URL_PREFIX) || url.startsWith(URL_PREFIX1));
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }

        if (acceptsURL(url)) {
            info = parseURL(url, info);
        }

        return getPropertyInfo(info);
    }

    /**
     * example: jdbc:TAOS://127.0.0.1:0/db?user=root&password=your_password
     */
    @Override
    public Properties parseURL(String url, Properties defaults) {
        Properties urlProps = (defaults != null) ? defaults : new Properties();
        if (url == null || url.length() <= 0 || url.trim().length() <= 0)
            return null;
        if (!url.startsWith(URL_PREFIX) && !url.startsWith(URL_PREFIX1))
            return null;

        // parse properties
        String urlForMeta = url;
        int beginningOfSlashes = url.indexOf("//");
        int index = url.indexOf("?");
        if (index != -1) {
            String paramString = url.substring(index + 1, url.length());
            url = url.substring(0, index);
            StringTokenizer queryParams = new StringTokenizer(paramString, "&");
            while (queryParams.hasMoreElements()) {
                String parameterValuePair = queryParams.nextToken();
                int indexOfEqual = parameterValuePair.indexOf("=");
                String parameter = null;
                String value = null;
                if (indexOfEqual != -1) {
                    parameter = parameterValuePair.substring(0, indexOfEqual);
                    if (indexOfEqual + 1 < parameterValuePair.length()) {
                        value = parameterValuePair.substring(indexOfEqual + 1);
                    }
                }
                if ((value != null && value.length() > 0) && (parameter != null && parameter.length() > 0)) {
                    urlProps.setProperty(parameter, value);
                }
            }
        }
        // parse Product Name
        String dbProductName = url.substring(0, beginningOfSlashes);
        dbProductName = dbProductName.substring(dbProductName.indexOf(":") + 1);
        dbProductName = dbProductName.substring(0, dbProductName.indexOf(":"));
        // parse dbname
        url = url.substring(beginningOfSlashes + 2);
        int indexOfSlash = url.indexOf("/");
        if (indexOfSlash != -1) {
            if (indexOfSlash + 1 < url.length()) {
                urlProps.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME, url.substring(indexOfSlash + 1));
            }
            url = url.substring(0, indexOfSlash);
        }
        // parse port
        int indexOfColon = url.indexOf(":");
        if (indexOfColon != -1) {
            if (indexOfColon + 1 < url.length()) {
                urlProps.setProperty(TSDBDriver.PROPERTY_KEY_PORT, url.substring(indexOfColon + 1));
            }
            url = url.substring(0, indexOfColon);
        }
        if (url != null && url.length() > 0 && url.trim().length() > 0) {
            urlProps.setProperty(TSDBDriver.PROPERTY_KEY_HOST, url);
        }
        this.dbMetaData = new TSDBDatabaseMetaData(dbProductName, urlForMeta, urlProps.getProperty(TSDBDriver.PROPERTY_KEY_USER));
        return urlProps;
    }

    public int getMajorVersion() {
        return 2;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
