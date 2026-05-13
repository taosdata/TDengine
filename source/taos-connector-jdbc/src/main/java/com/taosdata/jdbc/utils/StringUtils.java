package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Endpoint;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public class StringUtils {
    private StringUtils() {
    }

    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    /**
     * check string every char is numeric or false
     * so string is negative number or include decimal point，will return false
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        if (isEmpty(str)) {
            return false;
        }

        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static List<Endpoint> parseEndpoints(String endpointsFullStr) throws SQLException {
        if (StringUtils.isEmpty(endpointsFullStr)) {
            List<Endpoint> result = new ArrayList<>(1);
            Endpoint endpoint = new Endpoint("localhost", 0, false);
            result.add(endpoint);
            return result;
        }

        String[] endpoints = endpointsFullStr.split(",");
        List<Endpoint> result = new ArrayList<>(endpoints.length);

        for (int i = 0; i < endpoints.length; i++) {
            // parse host and port
            String endpointStr = endpoints[i];
            String host = "localhost";
            String port = "0";

            // ipv6 address
            if (endpointStr.startsWith("[")) {
                int endBracket = endpointStr.indexOf(']');
                if (endBracket != -1) {
                    // extract host
                    host = endpointStr.substring(0, endBracket + 1);

                    // if have port
                    if (endBracket + 1 < endpointStr.length() &&
                            endpointStr.charAt(endBracket + 1) == ':') {
                        port = endpointStr.substring(endBracket + 2);
                    }
                } else {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Invalid IPv6 address: " + endpointStr);
                }

                // handle Scope Identifier like fe80::1%eth0
                if (host.contains("%")) {
                    host = host.replace("%", "%25");
                }

                result.add(new Endpoint(host, Integer.parseInt(port), true));
            }
            // ipv4 address or hostname
            else {
                // find last ':'
                int lastColon = endpointStr.lastIndexOf(':');

                if (lastColon != -1) {
                    host = endpointStr.substring(0, lastColon);
                    String portStr = endpointStr.substring(lastColon + 1);
                    if (!StringUtils.isEmpty(portStr)){
                        port = portStr;
                    }
                } else {
                    host = endpointStr;
                }
                result.add(new Endpoint(host, Integer.parseInt(port), false));
            }
        }

        return result;
    }
    public static Properties parseUrl(String url, Properties defaults) throws SQLException {
        Properties urlProps = (defaults != null) ? defaults : new Properties();
        if (StringUtils.isEmpty(url)) {
            return urlProps;
        }

        // parse parameters
        int paramStart = url.indexOf("?");
        if (paramStart != -1) {
            String paramString = url.substring(paramStart + 1);
            url = url.substring(0, paramStart);
            StringTokenizer queryParams = new StringTokenizer(paramString, "&");
            while (queryParams.hasMoreElements()) {
                String pair = queryParams.nextToken();
                int eqIdx = pair.indexOf("=");
                if (eqIdx != -1) {
                    String key = pair.substring(0, eqIdx);
                    String value = eqIdx + 1 < pair.length() ? pair.substring(eqIdx + 1) : "";
                    if (!key.isEmpty() && !value.isEmpty()) {
                        urlProps.setProperty(key, value);
                    }
                }
            }
        }

        // parse product name
        int slashesStart = url.indexOf("//");
        String dbProductName = url.substring(0, slashesStart);
        dbProductName = dbProductName.substring(dbProductName.indexOf(":") + 1);
        dbProductName = dbProductName.substring(0, dbProductName.indexOf(":"));
        urlProps.setProperty(TSDBDriver.PROPERTY_KEY_PRODUCT_NAME, dbProductName);

        // extract host,port and dbname
        String hostPortDb = url.substring(slashesStart + 2);

        // parse dbname
        int dbStart = hostPortDb.indexOf("/");
        if (dbStart != -1) {
            if (dbStart + 1 < hostPortDb.length()) {
                urlProps.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME,
                        hostPortDb.substring(dbStart + 1).toLowerCase());
            }
            hostPortDb = hostPortDb.substring(0, dbStart);
        }

        urlProps.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, hostPortDb);

        if (!StringUtils.isEmpty(hostPortDb)) {
            List<Endpoint> endpoints = parseEndpoints(hostPortDb);
            if (endpoints.size() == 1) {
                Endpoint endpoint = endpoints.get(0);
                if (!StringUtils.isEmpty(endpoint.getHost())){
                    urlProps.setProperty(TSDBDriver.PROPERTY_KEY_HOST, endpoint.getHost());
                }
                if (endpoint.getPort() > 0) {
                    urlProps.setProperty(TSDBDriver.PROPERTY_KEY_PORT, String.valueOf(endpoint.getPort()));
                }
            }
        }
        return urlProps;
    }


    public static byte[] hexToBytes(String hex)
    {
        int byteLen = hex.length() / 2;
        byte[] bytes = new byte[byteLen];

        for (int i = 0; i < hex.length() / 2; i++) {
            int i2 = 2 * i;
            if (i2 + 1 > hex.length())
                throw new IllegalArgumentException("Hex string has odd length");

            int nib1 = hexToInt(hex.charAt(i2));
            int nib0 = hexToInt(hex.charAt(i2 + 1));
            byte b = (byte) ((nib1 << 4) | nib0);

            bytes[i] = b;
        }
        return bytes;
    }
    private static int hexToInt(char hex)
    {
        int nib = Character.digit(hex, 16);
        if (nib < 0)
            throw new IllegalArgumentException("Invalid hex digit: '" + hex + "'");
        return nib;
    }

    public static String bytesToHex(byte[] bytes)
    {
        return toHex(bytes);
    }

    /**
     * Converts a byte array to a hexadecimal string.
     *
     * @param bytes a byte array
     * @return a string of hexadecimal digits
     */
    public static String toHex(byte[] bytes)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            buf.append(toHexDigit((b >> 4) & 0x0F));
            buf.append(toHexDigit(b & 0x0F));
        }
        return buf.toString();
    }

    private static char toHexDigit(int n)
    {
        if (n < 0 || n > 15)
            throw new IllegalArgumentException("Nibble value out of range: " + n);
        if (n <= 9)
            return (char) ('0' + n);
        return (char) ('A' + (n - 10));
    }

    public static String getBasicUrl(String url){
        if (url == null || url.trim().length() == 0) {
            return "";
        }
        int firstIndex = url.indexOf("?");
        if (firstIndex > 0) {
            return url.substring(0, firstIndex);
        }
        return url;
    }

    public static String retainHostPortPart(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return jdbcUrl;
        }

        // Step 1: Remove all parameters after the question mark
        String urlWithoutParams = getBasicUrl(jdbcUrl);

        // Step 2: Find the position of "//" (protocol separator)
        int doubleSlashIndex = urlWithoutParams.indexOf("//");
        if (doubleSlashIndex == -1) {
            // No "//" in URL (e.g., jdbc:TAOS:/), return as is
            return urlWithoutParams;
        }

        // Step 3: Search for the first "/" after "//" (database name separator)
        int dbSeparatorIndex = urlWithoutParams.indexOf('/', doubleSlashIndex + 2); // +2 to skip "//"
        if (dbSeparatorIndex == -1) {
            // No database name separator found, return full URL without parameters
            return urlWithoutParams;
        }

        // Step 4: Extract part before the database separator
        return urlWithoutParams.substring(0, dbSeparatorIndex);
    }
}
