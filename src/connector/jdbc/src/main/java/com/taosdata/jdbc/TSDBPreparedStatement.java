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

import com.taosdata.jdbc.utils.Utils;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * TDengine only supports a subset of the standard SQL, thus this implementation of the
 * standard JDBC API contains more or less some adjustments customized for certain
 * compatibility needs.
 */
public class TSDBPreparedStatement extends TSDBStatement implements PreparedStatement {
    private String rawSql;
    private Object[] parameters;
    private boolean isPrepared;
    
    private ArrayList<ColumnInfo> colData;
    private String tableName;
    private long nativeStmtHandle = 0;
    
    private volatile TSDBParameterMetaData parameterMetaData;

    TSDBPreparedStatement(TSDBConnection connection, String sql) {
		super(connection);
        init(sql);

        int parameterCnt = 0;
        if (sql.contains("?")) {
            for (int i = 0; i < sql.length(); i++) {
                if ('?' == sql.charAt(i)) {
                    parameterCnt++;
                }
            }
            parameters = new Object[parameterCnt];
            this.isPrepared = true;
        }
        
        // the table name is also a parameter, so ignore it.
        this.colData = new ArrayList<ColumnInfo>(parameterCnt - 1);
        this.colData.addAll(Collections.nCopies(parameterCnt - 1, null));
    }

    private void init(String sql) {
        this.rawSql = sql;
        preprocessSql();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return super.executeBatch();
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
     *
     * @return a string of the native sql statement for TSDB
     */
    private String getNativeSql(String rawSql) throws SQLException {
        return Utils.getNativeSql(rawSql, this.parameters);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (!isPrepared)
            return executeQuery(this.rawSql);

        final String sql = getNativeSql(this.rawSql);
        return executeQuery(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (!isPrepared)
            return executeUpdate(this.rawSql);
        String sql = getNativeSql(this.rawSql);
        return executeUpdate(sql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null);
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
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
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
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void clearParameters() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        parameters = new Object[parameters.length];
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        
        if (parameterIndex < 1 && parameterIndex >= parameters.length) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE);
        }
        
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public boolean execute() throws SQLException {
        if (!isPrepared)
            return execute(this.rawSql);

        final String sql = getNativeSql(this.rawSql);
        return execute(sql);
    }

    @Override
    public void addBatch() throws SQLException {
        if (this.batchedArgs == null) {
            batchedArgs = new ArrayList<>();
        }

        if (!isPrepared) {
            addBatch(this.rawSql);
        } else {
            String sql = this.getConnection().nativeSQL(this.rawSql);
            addBatch(sql);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }
        
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        // TODO：
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        if (parameterMetaData == null) {
            this.parameterMetaData = new TSDBParameterMetaData(parameters);
        }
        return this.parameterMetaData;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        //TODO:
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }
    
    ///////////////////////////////////////////////////////////////////////
    // NOTE: the following APIs are not JDBC compatible
    // set the bind table name
    private static class ColumnInfo {
    	@SuppressWarnings("rawtypes")
		private ArrayList data;
    	private int       type;
    	private int       bytes;
    	private boolean   typeIsSet;
    	
    	public ColumnInfo() {
    		this.typeIsSet = false;
    	}
    	
    	public void setType(int type) throws SQLException {
    		if (this.isTypeSet()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "column data type has been set");
    		}

    		this.typeIsSet = true;
    		this.type = type;
    	}
    	
    	public boolean isTypeSet() {
    		return this.typeIsSet;
    	}
    };
    
    public void setTableName(String name) {
    	this.tableName = name;
    }
    
    public <T> void setValueImpl(int columnIndex, ArrayList<T> list, int type, int bytes) throws SQLException {
    	ColumnInfo col = (ColumnInfo) this.colData.get(columnIndex);
    	if (col == null) {
    		ColumnInfo p = new ColumnInfo();
    		p.setType(type);
    		p.bytes = bytes;
    		p.data = (ArrayList<?>) list.clone();
    		this.colData.set(columnIndex, p);
    	} else {
    		if (col.type != type) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "column data type mismatch");
    		}
    		col.data.addAll(list);
    	}
    }
    
    public void setInt(int columnIndex, ArrayList<Integer> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_INT, Integer.BYTES);
    }
    
    public void setFloat(int columnIndex, ArrayList<Float> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_FLOAT, Float.BYTES);
    }
    
    public void setTimestamp(int columnIndex, ArrayList<Long> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP, Long.BYTES);
    }
    
    public void setLong(int columnIndex, ArrayList<Long> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BIGINT, Long.BYTES);
    }
    
    public void setDouble(int columnIndex, ArrayList<Double> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_DOUBLE, Double.BYTES);
    }
    
    public void setBoolean(int columnIndex, ArrayList<Boolean> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BOOL, Byte.BYTES);
    }
    
    public void setByte(int columnIndex, ArrayList<Byte> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_TINYINT, Byte.BYTES);
    }
    
    public void setShort(int columnIndex, ArrayList<Short> list) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_SMALLINT, Short.BYTES);
    }
    
    public void setString(int columnIndex, ArrayList<String> list, int size) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_BINARY, size);
    }
    
    // note: expand the required space for each NChar character
    public void setNString(int columnIndex, ArrayList<String> list, int size) throws SQLException {
    	setValueImpl(columnIndex, list, TSDBConstants.TSDB_DATA_TYPE_NCHAR, size * Integer.BYTES);
    }
    
    public void columnDataAddBatch() {
    	// do nothing
    }
    
	public void columnDataExecuteBatch() throws SQLException {
		int numOfCols = this.colData.size();
		int rows = ((ColumnInfo) this.colData.get(0)).data.size();

		// pass the data block to native code
		TSDBJNIConnector connector = null;
		try {
			connector = ((TSDBConnection) this.getConnection()).getConnector();
			this.nativeStmtHandle = connector.prepareStmt(rawSql);
			
			// table name is not set yet, abort
			if (this.tableName == null) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "table name not set yet");
			}
			connector.setBindTableName(this.nativeStmtHandle, this.tableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < numOfCols; ++i) {
			ColumnInfo col1 = this.colData.get(i);
			if (col1 == null || !col1.isTypeSet()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "column data not bind");
			}
			
			if (rows != col1.data.size()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "the rows in column data not identical");
			}
			
			ByteBuffer colDataList = ByteBuffer.allocate(rows * col1.bytes);
			colDataList.order(ByteOrder.LITTLE_ENDIAN);
			
            ByteBuffer lengthList = ByteBuffer.allocate(rows * Integer.BYTES);
            lengthList.order(ByteOrder.LITTLE_ENDIAN);
            
            ByteBuffer isNullList = ByteBuffer.allocate(rows * Byte.BYTES);
            isNullList.order(ByteOrder.LITTLE_ENDIAN);
            
			switch (col1.type) {
				case TSDBConstants.TSDB_DATA_TYPE_INT: {
					for (int j = 0; j < rows; ++j) {
						Integer val = (Integer) col1.data.get(j);
						colDataList.putInt(val == null? Integer.MIN_VALUE:val);						
						isNullList.put((byte) (val == null? 1:0));
					}
					break;
				}
				
				case TSDBConstants.TSDB_DATA_TYPE_TINYINT: {
					for (int j = 0; j < rows; ++j) {
						Byte val = (Byte) col1.data.get(j);
						colDataList.put(val == null? 0:val);
						isNullList.put((byte) (val == null? 1:0));
					}
					break;
				}
				
				case TSDBConstants.TSDB_DATA_TYPE_BOOL: {
					for (int j = 0; j < rows; ++j) {
						Byte val = (Byte) col1.data.get(j);
						colDataList.put(val == null? 0:val);
						isNullList.put((byte) (val == null? 1:0));
					}
					break;
				}
				
				case TSDBConstants.TSDB_DATA_TYPE_SMALLINT: {
					for (int j = 0; j < rows; ++j) {
						Short val = (Short) col1.data.get(j);
						colDataList.putShort(val == null? 0:val);
						isNullList.put((byte) (val == null? 1:0));
					}
					break;
				}
				
				case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
				case TSDBConstants.TSDB_DATA_TYPE_BIGINT: {
					for (int j = 0; j < rows; ++j) {
						Long val = (Long) col1.data.get(j);
						colDataList.putLong(val == null? 0:val);
						isNullList.put((byte) (val == null? 1:0));
					}
					break;
				}
				
				case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
					for (int j = 0; j < rows; ++j) {
						Float val = (Float) col1.data.get(j);
						colDataList.putFloat(val == null? 0:val);
						isNullList.put((byte) (val == null? 1:0));
					}
					break;
				}
	
				case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
					for (int j = 0; j < rows; ++j) {
						Double val = (Double) col1.data.get(j);
						colDataList.putDouble(val == null? 0:val);
						isNullList.put((byte) (val == null? 1:0));
					}
					break;
				}
				
				case TSDBConstants.TSDB_DATA_TYPE_NCHAR: 
				case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
					String charset = TaosGlobalConfig.getCharset();
					for (int j = 0; j < rows; ++j) {
						String val = (String) col1.data.get(j);
						if (val != null && val.length() > col1.bytes) {
			                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "string data too long");
						}

						colDataList.position(j * col1.bytes);  // seek to the correct position
						if (val != null) {
							byte[] b = null;
							try {
								if (col1.type == TSDBConstants.TSDB_DATA_TYPE_BINARY) {
									b = val.getBytes();
								} else {
									b = val.getBytes(charset);
								}
							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();
							}
							
							colDataList.put(b);
							lengthList.putInt(b.length);
							isNullList.put((byte) 0);
						} else {
							lengthList.putInt(0);
							isNullList.put((byte) 1);
						}
					}
					break;
				}
				
				case TSDBConstants.TSDB_DATA_TYPE_UTINYINT:
				case TSDBConstants.TSDB_DATA_TYPE_USMALLINT:
				case TSDBConstants.TSDB_DATA_TYPE_UINT:
				case TSDBConstants.TSDB_DATA_TYPE_UBIGINT: {
	                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "not support data types");
				}
			};
			
			connector.bindColumnDataArray(this.nativeStmtHandle, colDataList, lengthList, isNullList, col1.type, col1.bytes, rows, i);
		}
		
		connector.executeBatch(this.nativeStmtHandle);
	}
    
    public void columnDataClearBatch() {
    	// TODO clear data in this.colData
    }
    
    public void columnDataCloseBatch() {
    	TSDBJNIConnector connector = null;
		try {
			connector = ((TSDBConnection) this.getConnection()).getConnector();
			connector.closeBatch(this.nativeStmtHandle);
			this.nativeStmtHandle = 0L;
			this.tableName = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
}
