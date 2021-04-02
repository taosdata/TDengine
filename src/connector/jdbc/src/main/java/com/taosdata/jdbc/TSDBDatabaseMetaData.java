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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TSDBDatabaseMetaData extends AbstractDatabaseMetaData {

    private String url;
    private String userName;
    private Connection conn;

    public TSDBDatabaseMetaData(String url, String userName) {
        this.url = url;
        this.userName = userName;
    }

    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public String getURL() throws SQLException {
        return this.url;
    }

    public String getUserName() throws SQLException {
        return this.userName;
    }

    public String getDriverName() throws SQLException {
        return TSDBDriver.class.getName();
    }

    /**
     * @Param catalog : database名称，"" 表示不属于任何database的table，null表示不使用database来缩小范围
     * @Param schemaPattern : schema名称，""表示
     * @Param tableNamePattern : 表名满足tableNamePattern的表, null表示返回所有表
     * @Param types : 表类型，null表示返回所有类型
     */
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (conn == null || conn.isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return super.getTables(catalog, schemaPattern, tableNamePattern, types, conn);
    }

    public ResultSet getCatalogs() throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getCatalogs(conn);
    }

    public ResultSet getTableTypes() throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getTableTypes();
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern, conn);
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getPrimaryKeys(catalog, schema, table, conn);
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        if (conn == null || conn.isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return super.getSuperTables(catalog, schemaPattern, tableNamePattern, conn);
    }

}