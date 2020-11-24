package com.taosdata.jdbc;

import java.io.*;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public abstract class AbstractTaosDriver implements Driver {

    private static final String TAOS_CFG_FILENAME = "taos.cfg";

    /**
     * @param cfgDirPath
     * @return return the config dir
     **/
    protected File loadConfigDir(String cfgDirPath) {
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
    protected File loadDefaultConfigDir() {
        File cfgDir;
        File cfgDir_linux = new File("/etc/taos");
        cfgDir = cfgDir_linux.exists() ? cfgDir_linux : null;
        File cfgDir_windows = new File("C:\\TDengine\\cfg");
        cfgDir = (cfgDir == null && cfgDir_windows.exists()) ? cfgDir_windows : cfgDir;
        return cfgDir;
    }

    protected List<String> loadConfigEndpoints(File cfgFile) {
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

    protected void loadTaosConfig(Properties info) {
        if ((info.getProperty(TSDBDriver.PROPERTY_KEY_HOST) == null ||
                info.getProperty(TSDBDriver.PROPERTY_KEY_HOST).isEmpty()) && (
                info.getProperty(TSDBDriver.PROPERTY_KEY_PORT) == null ||
                        info.getProperty(TSDBDriver.PROPERTY_KEY_PORT).isEmpty())) {
            File cfgDir = loadConfigDir(info.getProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR));
            File cfgFile = cfgDir.listFiles((dir, name) -> TAOS_CFG_FILENAME.equalsIgnoreCase(name))[0];
            List<String> endpoints = loadConfigEndpoints(cfgFile);
            if (!endpoints.isEmpty()) {
                info.setProperty(TSDBDriver.PROPERTY_KEY_HOST, endpoints.get(0).split(":")[0]);
                info.setProperty(TSDBDriver.PROPERTY_KEY_PORT, endpoints.get(0).split(":")[1]);
            }
        }
    }

    protected DriverPropertyInfo[] getPropertyInfo(Properties info) {
        DriverPropertyInfo hostProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_HOST, info.getProperty(TSDBDriver.PROPERTY_KEY_HOST));
        hostProp.required = false;
        hostProp.description = "Hostname";

        DriverPropertyInfo portProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_PORT, info.getProperty(TSDBDriver.PROPERTY_KEY_PORT, TSDBConstants.DEFAULT_PORT));
        portProp.required = false;
        portProp.description = "Port";

        DriverPropertyInfo dbProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_DBNAME, info.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME));
        dbProp.required = false;
        dbProp.description = "Database name";

        DriverPropertyInfo userProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_USER, info.getProperty(TSDBDriver.PROPERTY_KEY_USER));
        userProp.required = true;
        userProp.description = "User";

        DriverPropertyInfo passwordProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_PASSWORD, info.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
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

    protected Properties parseURL(String url, Properties defaults) {
        Properties urlProps = (defaults != null) ? defaults : new Properties();

        // parse properties
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
        // parse host
        if (url != null && url.length() > 0 && url.trim().length() > 0) {
            urlProps.setProperty(TSDBDriver.PROPERTY_KEY_HOST, url);
        }
        return urlProps;
    }



}
