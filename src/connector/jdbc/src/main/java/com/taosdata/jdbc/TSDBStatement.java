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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TSDBStatement extends AbstractStatement {

    private TSDBJNIConnector connector;

    /**
     * To store batched commands
     */
    protected List<String> batchedArgs;
//    private Long pSql = 0l;
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

        TSDBResultSet res = new TSDBResultSet(this.connector, pSql);
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
        if (!isClosed) {
            if (this.resultSet != null)
                this.resultSet.close();
            isClosed = true;
        }
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

        this.resultSet = new TSDBResultSet(this.connector, pSql);
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

    public int getFetchDirection() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.resultSet.getFetchDirection();
    }

    /*
     * used by spark
     */
    public int getFetchSize() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.resultSet.getFetchSize();
    }

    public int getResultSetConcurrency() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.resultSet.getConcurrency();
    }

    public int getResultSetType() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.resultSet.getType();
    }

    public void addBatch(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        if (batchedArgs == null) {
            batchedArgs = new ArrayList<>();
        }
        batchedArgs.add(sql);
    }

    public void clearBatch() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        batchedArgs.clear();
    }

    public int[] executeBatch() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (batchedArgs == null || batchedArgs.isEmpty())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_BATCH_IS_EMPTY);

        int[] res = new int[batchedArgs.size()];
        for (int i = 0; i < batchedArgs.size(); i++) {
            boolean isSelect = execute(batchedArgs.get(i));
            if (isSelect) {
                res[i] = SUCCESS_NO_INFO;
            } else {
                res[i] = getUpdateCount();
            }
        }
        return res;
    }

    public Connection getConnection() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (this.connector == null)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        return this.connection;
    }

    public boolean getMoreResults(int current) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw new SQLException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    public int getResultSetHoldability() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.resultSet.getHoldability();
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }


}
