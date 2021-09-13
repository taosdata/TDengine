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
package com.taosdata.jdbc.utils;

public class SqlSyntaxValidator {

    private static final String[] updateSQL = {"insert", "import", "create", "use", "alter", "drop", "set", "reset"};
    private static final String[] querySQL = {"select", "show", "describe"};

    private static final String[] databaseUnspecifiedShow = {"databases", "dnodes", "mnodes", "variables"};

    public static boolean isValidForExecuteUpdate(String sql) {
        for (String prefix : updateSQL) {
            if (sql.trim().toLowerCase().startsWith(prefix))
                return true;
        }
        return false;
    }

    public static boolean isValidForExecuteQuery(String sql) {
        for (String prefix : querySQL) {
            if (sql.trim().toLowerCase().startsWith(prefix))
                return true;
        }
        return false;
    }

    public static boolean isDatabaseUnspecifiedQuery(String sql) {
        for (String databaseObj : databaseUnspecifiedShow) {
            if (sql.trim().toLowerCase().matches("show\\s+" + databaseObj + ".*"))
                return true;
        }
        return false;
    }

    public static boolean isDatabaseUnspecifiedUpdate(String sql) {
        sql = sql.trim().toLowerCase();
        return sql.matches("create\\s+database.*") || sql.startsWith("set") || sql.matches("drop\\s+database.*");
    }

    public static boolean isUseSql(String sql) {
        return sql.trim().toLowerCase().startsWith("use");
    }


}
