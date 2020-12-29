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

import com.taosdata.jdbc.TSDBConnection;

import java.sql.Connection;

public class SqlSyntaxValidator {

    private static final String[] updateSQL = {"insert", "update", "delete", "create", "alter", "drop", "show", "describe", "use", "import"};
    private static final String[] querySQL = {"select"};

    private TSDBConnection tsdbConnection;

    public SqlSyntaxValidator(Connection connection) {
        this.tsdbConnection = (TSDBConnection) connection;
    }

    public static boolean isValidForExecuteUpdate(String sql) {
        for (String prefix : updateSQL) {
            if (sql.trim().toLowerCase().startsWith(prefix))
                return true;
        }
        return false;
    }

    public static boolean isUseSql(String sql) {
        return sql.trim().toLowerCase().startsWith("use") || sql.trim().toLowerCase().matches("create\\s*database.*") || sql.toLowerCase().toLowerCase().matches("drop\\s*database.*");
    }

    public static boolean isShowSql(String sql) {
        return sql.trim().toLowerCase().startsWith("show");
    }

    public static boolean isDescribeSql(String sql) {
        return sql.trim().toLowerCase().startsWith("describe");
    }


    public static boolean isInsertSql(String sql) {
        return sql.trim().toLowerCase().startsWith("insert") || sql.trim().toLowerCase().startsWith("import");
    }

    public static boolean isSelectSql(String sql) {
        return sql.trim().toLowerCase().startsWith("select");
    }


    public static boolean isShowDatabaseSql(String sql) {
        return sql.trim().toLowerCase().matches("show\\s*databases");
    }
}
