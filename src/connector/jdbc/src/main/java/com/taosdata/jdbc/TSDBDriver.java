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

import java.io.*;
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
public class TSDBDriver implements java.sql.Driver {

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

    private List<String> loadConfigEndpoints(File cfgFile) {
        List<String> endpoints = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(cfgFile))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.trim().startsWith("firstEp") || line.trim().startsWith("secondEp")) {
                    endpoints.add(line.substring(line.indexOf('p') + 1).trim());
                }
                if (endpoints.size() > 1)
                    break;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return endpoints;
    }

    /**
     * @param cfgDirPath
     * @return return the config dir
     **/
    private File loadConfigDir(String cfgDirPath) {
        if (cfgDirPath == null)
            return loadDefaultConfigDir();
        File cfgDir = new File(cfgDirPath);
        if (!cfgDir.exists())
            return loadDefaultConfigDir();
        return cfgDir;
    }

    /**
     * @return search the default config dir, if the config dir is not exist will return null
     */
    private File loadDefaultConfigDir() {
        File cfgDir;
        File cfgDir_linux = new File("/etc/taos");
        cfgDir = cfgDir_linux.exists() ? cfgDir_linux : null;
        File cfgDir_windows = new File("C:\\TDengine\\cfg");
        cfgDir = (cfgDir == null && cfgDir_windows.exists()) ? cfgDir_windows : cfgDir;
        return cfgDir;
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
        if ((info.getProperty(TSDBDriver.PROPERTY_KEY_HOST) == null ||
                info.getProperty(TSDBDriver.PROPERTY_KEY_HOST).isEmpty()) && (
                info.getProperty(TSDBDriver.PROPERTY_KEY_PORT) == null ||
                        info.getProperty(TSDBDriver.PROPERTY_KEY_PORT).isEmpty())) {
            File cfgDir = loadConfigDir(info.getProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR));
            File cfgFile = cfgDir.listFiles((dir, name) -> "taos.cfg".equalsIgnoreCase(name))[0];
            List<String> endpoints = loadConfigEndpoints(cfgFile);
            if (!endpoints.isEmpty()) {
                info.setProperty(TSDBDriver.PROPERTY_KEY_HOST, endpoints.get(0).split(":")[0]);
                info.setProperty(TSDBDriver.PROPERTY_KEY_PORT, endpoints.get(0).split(":")[1]);
            }
        }

        try {

            Enumeration<?> propertyNames = props.propertyNames();
            while (propertyNames.hasMoreElements()) {
                Object name = propertyNames.nextElement();
                System.out.println(name + " : " + props.get(name));
            }

            TSDBJNIConnector.init((String) props.get(PROPERTY_KEY_CONFIG_DIR),
                    (String) props.get(PROPERTY_KEY_LOCALE),
                    (String) props.get(PROPERTY_KEY_CHARSET),
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

        DriverPropertyInfo hostProp = new DriverPropertyInfo(PROPERTY_KEY_HOST, info.getProperty(PROPERTY_KEY_HOST));
        hostProp.required = false;
        hostProp.description = "Hostname";

        DriverPropertyInfo portProp = new DriverPropertyInfo(PROPERTY_KEY_PORT, info.getProperty(PROPERTY_KEY_PORT, TSDBConstants.DEFAULT_PORT));
        portProp.required = false;
        portProp.description = "Port";

        DriverPropertyInfo dbProp = new DriverPropertyInfo(PROPERTY_KEY_DBNAME, info.getProperty(PROPERTY_KEY_DBNAME));
        dbProp.required = false;
        dbProp.description = "Database name";

        DriverPropertyInfo userProp = new DriverPropertyInfo(PROPERTY_KEY_USER, info.getProperty(PROPERTY_KEY_USER));
        userProp.required = true;
        userProp.description = "User";

        DriverPropertyInfo passwordProp = new DriverPropertyInfo(PROPERTY_KEY_PASSWORD, info.getProperty(PROPERTY_KEY_PASSWORD));
        passwordProp.required = true;
        passwordProp.description = "Password";

        DriverPropertyInfo[] propertyInfo = new DriverPropertyInfo[5];
        propertyInfo[0] = hostProp;
        propertyInfo[1] = portProp;
        propertyInfo[2] = dbProp;
        propertyInfo[3] = userProp;
        propertyInfo[4] = passwordProp;

        return propertyInfo;
    }

    /**
     * example: jdbc:TAOS://127.0.0.1:0/db?user=root&password=your_password
     */
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

        /*
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
            if (!url.trim().isEmpty()) {
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
            if (kvPair.length < 2) {
                continue;
            }
            setPropertyValue(urlProps, kvPair);
        }

        user = urlProps.getProperty(PROPERTY_KEY_USER).toString();
        this.dbMetaData = new TSDBDatabaseMetaData(dbProductName, urlForMeta, user);
*/
        return urlProps;
    }

    private void setPropertyValue(Properties property, String[] keyValuePair) {
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
