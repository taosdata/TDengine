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

public class TSDBStatement extends AbstractStatement {

    private TSDBJNIConnector connector;
    /**
     * Status of current statement
     */
    private boolean isClosed;
    private int affectedRows = -1;
    private TSDBConnection connection;
    private TSDBResultSet resultSet;

    public void setConnection(TSDBConnection connection) {
        this.connection = connection;
    }

    TSDBStatement(TSDBConnection connection, TSDBJNIConnector connector) {
        this.connection = connection;
        this.connector = connector;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        // check if closed
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        //TODO: 如果在executeQuery方法中执行insert语句，那么先执行了SQL，再通过pSql来检查是否为一个insert语句，但这个insert SQL已经执行成功了

        // execute query
        long pSql = this.connector.executeQuery(sql);
        // if pSql is create/insert/update/delete/alter SQL
        if (this.connector.isUpdateQuery(pSql)) {
            this.connector.freeResultSet(pSql);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEQUERY);
        }

        TSDBResultSet res = new TSDBResultSet(this, this.connector, pSql);
        res.setBatchFetch(this.connection.getBatchFetch());
        return res;
    }

    public int executeUpdate(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        long pSql = this.connector.executeQuery(sql);
        // if pSql is create/insert/update/delete/alter SQL
        if (!this.connector.isUpdateQuery(pSql)) {
            this.connector.freeResultSet(pSql);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEUPDATE);
        }
        int affectedRows = this.connector.getAffectedRows(pSql);
        this.connector.freeResultSet(pSql);
        return affectedRows;
    }

    public void close() throws SQLException {
        if (isClosed)
            return;
        if (this.resultSet != null && !this.resultSet.isClosed())
            this.resultSet.close();
        isClosed = true;
    }

    public boolean execute(String sql) throws SQLException {
        // check if closed
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        // execute query
        long pSql = this.connector.executeQuery(sql);
        // if pSql is create/insert/update/delete/alter SQL
        if (this.connector.isUpdateQuery(pSql)) {
            this.affectedRows = this.connector.getAffectedRows(pSql);
            this.connector.freeResultSet(pSql);
            return false;
        }

        this.resultSet = new TSDBResultSet(this, this.connector, pSql);
        this.resultSet.setBatchFetch(this.connection.getBatchFetch());
        return true;
    }

    public ResultSet getResultSet() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
//        long resultSetPointer = connector.getResultSet();
//        TSDBResultSet resSet = null;
//        if (resultSetPointer != TSDBConstants.JNI_NULL_POINTER) {
//            resSet = new TSDBResultSet(connector, resultSetPointer);
//        }
        return this.resultSet;
    }

    public int getUpdateCount() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.affectedRows;
    }

    public Connection getConnection() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (this.connector == null)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        return this.connection;
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }


}
