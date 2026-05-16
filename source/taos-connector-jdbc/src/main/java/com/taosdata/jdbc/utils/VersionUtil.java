package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.VersionReq;
import com.taosdata.jdbc.ws.entity.VersionResp;

import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.*;

public class VersionUtil {
    private VersionUtil(){}

    public static boolean checkVersion(String version) throws SQLException {
        if (version != null) {
            return compareVersions(version, MIN_SUPPORT_VERSION) >= 0;
        }
        return false;
    }

    public static String getVersion(Transport transport) throws SQLException {
            VersionReq versionReq = new VersionReq();
            versionReq.setReqId(ReqId.getReqID());
            VersionResp versionResp = (VersionResp) transport.send(new Request(Action.VERSION.getAction(), versionReq));
            return versionResp.getVersion();
    }

    public static boolean surpportBlob(String version){
        if (version != null) {
            try {
                return compareVersions(version, MIN_BLOB_SUPPORT_VERSION) >= 0;
            } catch (SQLException ignored){
            }
        }
        return false;
    }

    public static int compareVersions(String v1, String v2) throws SQLException {
        if (v1 == null || v2 == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Version strings cannot be null");
        }

        String[] v1Parts = v1.split("[.-]");
        String[] v2Parts = v2.split("[.-]");

        int maxLength = Math.max(v1Parts.length, v2Parts.length);

        for (int i = 0; i < maxLength; i++) {
            String v1Part = i < v1Parts.length ? v1Parts[i] : "0";
            String v2Part = i < v2Parts.length ? v2Parts[i] : "0";

            if (i == v1Parts.length - 1 && containsNonDigit(v1Part)) {
                if (i < v2Parts.length && containsNonDigit(v2Part)) {
                    return v1Part.compareTo(v2Part);
                } else {
                    return -1;
                }
            } else if (i == v2Parts.length - 1 && containsNonDigit(v2Part)) {
                return 1;
            }

            int v1Num = parseVersionPart(v1Part);
            int v2Num = parseVersionPart(v2Part);

            if (v1Num != v2Num) {
                return v1Num - v2Num;
            }
        }

        return 0;
    }

    private static boolean containsNonDigit(String s) {
        for (char c : s.toCharArray()) {
            if (!Character.isDigit(c)) {
                return true;
            }
        }
        return false;
    }

    private static int parseVersionPart(String part) {
        try {
            return Integer.parseInt(part);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
