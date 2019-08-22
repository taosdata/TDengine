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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * TDengine only supports a subset of the standard SQL, thus this implemetation of the
 * standard JDBC API contains more or less some adjustments customized for certain
 * compatibility needs.
 */
public class TSDBPreparedStatement extends TSDBStatement implements PreparedStatement {
	protected String rawSql;
	protected String sql;
	protected ArrayList<Object> parameters = new ArrayList<Object>();

	TSDBPreparedStatement(TSDBJNIConnector connecter, String sql) {
        super(connecter);
        this.rawSql = sql;
        preprocessSql();
    }

    public ArrayList<Object> getParameters() {
        return parameters;
    }

    public void setParameters(ArrayList<Object> parameters) {
        this.parameters = parameters;
    }

    public String getRawSql() {
        return rawSql;
    }

    /*
     * Some of the SQLs sent by other popular frameworks or tools like Spark, contains syntax that cannot be parsed by
     * the TDengine client. Thus, some simple parsers/filters are intentionally added in this JDBC implementation in
     * order to process those supported SQLs.
     */
    private void preprocessSql() {

        /***** For processing some of Spark SQLs*****/
		// should replace it first
		this.rawSql = this.rawSql.replaceAll("or (.*) is null", "");
		this.rawSql = this.rawSql.replaceAll(" where ", " WHERE ");
		this.rawSql = this.rawSql.replaceAll(" or ", " OR ");
		this.rawSql = this.rawSql.replaceAll(" and ", " AND ");
		this.rawSql = this.rawSql.replaceAll(" is null", " IS NULL");
		this.rawSql = this.rawSql.replaceAll(" is not null", " IS NOT NULL");

		// SELECT * FROM db.tb WHERE 1=0
		this.rawSql = this.rawSql.replaceAll("WHERE 1=0", "WHERE _c0=1");
		this.rawSql = this.rawSql.replaceAll("WHERE 1=2", "WHERE _c0=1");

		// SELECT "ts","val" FROM db.tb
		this.rawSql = this.rawSql.replaceAll("\"", "");

		// SELECT 1 FROM db.tb
		this.rawSql = this.rawSql.replaceAll("SELECT 1 FROM", "SELECT * FROM");

		// SELECT "ts","val" FROM db.tb WHERE ts < 33 or ts is null
		this.rawSql = this.rawSql.replaceAll("OR (.*) IS NULL", "");

		// SELECT "ts","val" FROM db.tb WHERE ts is null or ts < 33
		this.rawSql = this.rawSql.replaceAll("(.*) IS NULL OR", "");

		// SELECT 1 FROM db.tb WHERE (("val" IS NOT NULL) AND ("val" > 50)) AND (ts >= 66)
		this.rawSql = this.rawSql.replaceAll("\\(\\((.*) IS NOT NULL\\) AND", "(");

		// SELECT 1 FROM db.tb WHERE ("val" IS NOT NULL) AND ("val" > 50) AND (ts >= 66)
		this.rawSql = this.rawSql.replaceAll("\\((.*) IS NOT NULL\\) AND", "");

		// SELECT "ts","val" FROM db.tb WHERE (("val" IS NOT NULL)) AND (ts < 33 or ts is null) 
		this.rawSql = this.rawSql.replaceAll("\\(\\((.*) IS NOT NULL\\)\\) AND", "");

		/***** For processing inner subqueries *****/
		Pattern pattern = Pattern.compile("FROM\\s+((\\(.+\\))\\s+SUB_QRY)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(rawSql);
        String tableFullName = "";
        if (matcher.find() && matcher.groupCount() == 2) {
            String subQry = matcher.group(2);
            Pattern pattern1 = Pattern.compile("FROM\\s+(\\w+\\.\\w+)", Pattern.CASE_INSENSITIVE);
            Matcher matcher1 = pattern1.matcher(subQry);
            if (matcher1.find() && matcher1.groupCount() == 1) {
                tableFullName = matcher1.group(1);
            }
            rawSql = rawSql.replace(matcher.group(1), tableFullName);
        }
        /***** for inner queries *****/

	}

    /**
     * Populate parameters into prepared sql statements
     * @return a string of the native sql statement for TSDB
     */
	private String getNativeSql() {
        this.sql = this.rawSql;
		for (int i = 0; i < parameters.size(); ++i) {
			Object para = parameters.get(i);
			if (para != null) {
			    String paraStr = para.toString();
			    if (para instanceof Timestamp || para instanceof String) {
			        paraStr = "'" + paraStr + "'";
                }
                this.sql = this.sql.replaceFirst("[?]", paraStr);
			} else {
				this.sql = this.sql.replaceFirst("[?]", "NULL");
			}
		}
		parameters.clear();
		return sql;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return super.executeQuery(getNativeSql());
	}

	@Override
	public int executeUpdate() throws SQLException {
		return super.executeUpdate(getNativeSql());
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		setObject(parameterIndex, new String("NULL"));
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void clearParameters() throws SQLException {
		parameters.clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
        parameters.add(x);
	}

	@Override
	public boolean execute() throws SQLException {
	    return super.execute(getNativeSql());
	}

	@Override
	public void addBatch() throws SQLException {
        if (this.batchedArgs == null) {
            batchedArgs = new ArrayList<String>();
        }
        super.addBatch(getNativeSql());
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
	}
}
