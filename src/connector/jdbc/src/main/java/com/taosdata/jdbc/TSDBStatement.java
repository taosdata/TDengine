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
    /**
     * Status of current statement
     */
    private boolean isClosed;
    private TSDBConnection connection;
    private TSDBResultSet resultSet;

    TSDBStatement(TSDBConnection connection) {
        this.connection = connection;
        connection.registerStatement(this);
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        synchronized (this) {
            if (isClosed()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
            }
            if (this.resultSet != null && !this.resultSet.isClosed())
                this.resultSet.close();
            //TODO:
            // this is an unreasonable implementation, if the paratemer is a insert statement,
            // the JNI connector will execute the sql at first and return a pointer: pSql,
            // we use this pSql and invoke the isUpdateQuery(long pSql) method to decide .
            // but the insert sql is already executed in database.
            //execute query
            long pSql = this.connection.getConnector().executeQuery(sql);
            // if pSql is create/insert/update/delete/alter SQL
            if (this.connection.getConnector().isUpdateQuery(pSql)) {
                this.connection.getConnector().freeResultSet(pSql);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEQUERY);
            }
            int timestampPrecision = this.connection.getConnector().getResultTimePrecision(pSql);
            resultSet = new TSDBResultSet(this, this.connection.getConnector(), pSql, timestampPrecision);
            resultSet.setBatchFetch(this.connection.getBatchFetch());
            return resultSet;
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        synchronized (this) {
            if (isClosed())
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
            if (this.resultSet != null && !this.resultSet.isClosed())
                this.resultSet.close();

            long pSql = this.connection.getConnector().executeQuery(sql);
            // if pSql is create/insert/update/delete/alter SQL
            if (!this.connection.getConnector().isUpdateQuery(pSql)) {
                this.connection.getConnector().freeResultSet(pSql);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEUPDATE);
            }
            int affectedRows = this.connection.getConnector().getAffectedRows(pSql);
            this.connection.getConnector().freeResultSet(pSql);
            return affectedRows;
        }
    }

    public void close() throws SQLException {
        if (isClosed)
            return;
        connection.unregisterStatement(this);
        if (this.resultSet != null && !this.resultSet.isClosed())
            this.resultSet.close();
        isClosed = true;
    }

    public boolean execute(String sql) throws SQLException {
        synchronized (this) {
            // check if closed
            if (isClosed()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
            }
            if (this.resultSet != null && !this.resultSet.isClosed())
                this.resultSet.close();

            // execute query
            long pSql = this.connection.getConnector().executeQuery(sql);
            // if pSql is create/insert/update/delete/alter SQL
            if (this.connection.getConnector().isUpdateQuery(pSql)) {
                this.affectedRows = this.connection.getConnector().getAffectedRows(pSql);
                this.connection.getConnector().freeResultSet(pSql);
                return false;
            }

            int timestampPrecision = this.connection.getConnector().getResultTimePrecision(pSql);
            this.resultSet = new TSDBResultSet(this, this.connection.getConnector(), pSql, timestampPrecision);
            this.resultSet.setBatchFetch(this.connection.getBatchFetch());
            return true;
        }
    }

    public ResultSet getResultSet() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

        return this.resultSet;
    }

    public int getUpdateCount() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.affectedRows;
    }

    public Connection getConnection() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

        if (this.connection.getConnector() == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        }

        return this.connection;
    }

    public void setConnection(TSDBConnection connection) {
        this.connection = connection;
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }


}
