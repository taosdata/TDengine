package com.taosdata.jdbc;

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.Properties;
import java.util.StringTokenizer;

public abstract class AbstractDriver implements Driver {

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
