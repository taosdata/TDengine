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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

public class TSDBResultSetMetaData extends WrapperImpl implements ResultSetMetaData {

	List<ColumnMetaData> colMetaDataList = null;

	public TSDBResultSetMetaData(List<ColumnMetaData> metaDataList) {
		this.colMetaDataList = metaDataList;
	}

	public int getColumnCount() throws SQLException {
		return colMetaDataList.size();
	}

	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	public boolean isCaseSensitive(int column) throws SQLException {
		return false;
	}

	public boolean isSearchable(int column) throws SQLException {
		if (column == 1) {
			return true;
		}
		return false;
	}

	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	public int isNullable(int column) throws SQLException {
		if (column == 1) {
			return columnNoNulls;
		}
		return columnNullable;
	}

	public boolean isSigned(int column) throws SQLException {
		ColumnMetaData meta = this.colMetaDataList.get(column - 1);
		switch (meta.getColType()) {
		case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
		case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
		case TSDBConstants.TSDB_DATA_TYPE_INT:
		case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
			return true;
		default:
			return false;
		}
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		return colMetaDataList.get(column - 1).getColSize();
	}

	public String getColumnLabel(int column) throws SQLException {
		return colMetaDataList.get(column - 1).getColName();
	}

	public String getColumnName(int column) throws SQLException {
		return colMetaDataList.get(column - 1).getColName();
	}

	public String getSchemaName(int column) throws SQLException {
		throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
	}

	public int getPrecision(int column) throws SQLException {
		ColumnMetaData columnMetaData = this.colMetaDataList.get(column - 1);
		switch (columnMetaData.getColType()) {
		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
			return 5;
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
			return 9;
		case TSDBConstants.TSDB_DATA_TYPE_BINARY:
		case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
			return columnMetaData.getColSize();
		default:
			return 0;
		}
	}

	public int getScale(int column) throws SQLException {
		ColumnMetaData meta = this.colMetaDataList.get(column - 1);
		switch (meta.getColType()) {
		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
			return 5;
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
			return 9;
		default:
			return 0;
		}
	}

	public String getTableName(int column) throws SQLException {
		throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
	}

	public String getCatalogName(int column) throws SQLException {
		throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
	}

	public int getColumnType(int column) throws SQLException {
		ColumnMetaData meta = this.colMetaDataList.get(column - 1);
		switch (meta.getColType()) {
		case TSDBConstants.TSDB_DATA_TYPE_BOOL:
			return Types.BOOLEAN;
		case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
			return java.sql.Types.TINYINT;
		case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
			return java.sql.Types.SMALLINT;
		case TSDBConstants.TSDB_DATA_TYPE_INT:
			return java.sql.Types.INTEGER;
		case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
			return java.sql.Types.BIGINT;
		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
			return java.sql.Types.FLOAT;
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
			return java.sql.Types.DOUBLE;
		case TSDBConstants.TSDB_DATA_TYPE_BINARY:
			return Types.BINARY;
		case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
			return java.sql.Types.TIMESTAMP;
		case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
			return Types.NCHAR;
		}
		throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
	}

	public String getColumnTypeName(int column) throws SQLException {
		ColumnMetaData meta = this.colMetaDataList.get(column - 1);
		return TSDBConstants.DATATYPE_MAP.get(meta.getColType());
	}

	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
	}

	public String getColumnClassName(int column) throws SQLException {
		int columnType = getColumnType(column);
		String columnClassName = "";
		switch (columnType) {
		    case Types.TIMESTAMP:
		        columnClassName = Timestamp.class.getName();
                break;
            case Types.CHAR:
                columnClassName = String.class.getName();
                break;
            case Types.DOUBLE:
                columnClassName = Double.class.getName();
                break;
            case Types.FLOAT:
                columnClassName = Float.class.getName();
                break;
            case Types.BIGINT:
                columnClassName = Long.class.getName();
                break;
            case Types.INTEGER:
                columnClassName = Integer.class.getName();
                break;
			case Types.SMALLINT:
				columnClassName = Short.class.getName();
                break;
            case Types.TINYINT:
                columnClassName = Byte.class.getName();
                break;
            case Types.BIT:
                columnClassName = Boolean.class.getName();
                break;
		}
        return columnClassName;
	}
}
