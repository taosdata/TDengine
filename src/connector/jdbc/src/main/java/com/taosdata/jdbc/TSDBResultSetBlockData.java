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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TSDBResultSetBlockData {
	private int numOfRows = 0;
	private int rowIndex = 0;

	private List<ColumnMetaData> columnMetaDataList;
	private ArrayList<Object> colData = null;

	public TSDBResultSetBlockData(List<ColumnMetaData> colMeta, int numOfCols) {
		this.columnMetaDataList = colMeta;
		this.colData = new ArrayList<Object>(numOfCols);
	}

	public TSDBResultSetBlockData() {
		this.colData = new ArrayList<Object>();
	}

	public void clear() {
		int size = this.colData.size();
		if (this.colData != null) {
			this.colData.clear();
		}
		
		setNumOfCols(size);
	}

	public int getNumOfRows() {
		return this.numOfRows;
	}

	public void setNumOfRows(int numOfRows) {
		this.numOfRows = numOfRows;
	}

	public int getNumOfCols() {
		return this.colData.size();
	}

	public void setNumOfCols(int numOfCols) {
		this.colData = new ArrayList<Object>(numOfCols);
		this.colData.addAll(Collections.nCopies(numOfCols, null));
	}

	public boolean hasMore() {
		return this.rowIndex < this.numOfRows;
	}

	public boolean forward() {
		if (this.rowIndex > this.numOfRows) {
			return false;
		}

		return ((++this.rowIndex) < this.numOfRows);
	}

	public void reset() {
		this.rowIndex = 0;
	}

	public void setBoolean(int col, boolean value) {
		colData.set(col, value);
	}

	public void setByteArray(int col, int length, byte[] value) {
		try {
			switch (this.columnMetaDataList.get(col).getColType()) {
			case TSDBConstants.TSDB_DATA_TYPE_BOOL: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				buf.order(ByteOrder.LITTLE_ENDIAN).asCharBuffer();
				this.colData.set(col, buf);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_TINYINT: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				buf.order(ByteOrder.LITTLE_ENDIAN);
				this.colData.set(col, buf);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_SMALLINT: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				ShortBuffer sb = buf.order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
				this.colData.set(col, sb);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_INT: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				IntBuffer ib = buf.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
				this.colData.set(col, ib);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_BIGINT: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				LongBuffer lb = buf.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
				this.colData.set(col, lb);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				FloatBuffer fb = buf.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
				this.colData.set(col, fb);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				DoubleBuffer db = buf.order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
				this.colData.set(col, db);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				buf.order(ByteOrder.LITTLE_ENDIAN);
				this.colData.set(col, buf);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				LongBuffer lb = buf.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
				this.colData.set(col, lb);
				break;
			}
			case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
				ByteBuffer buf = ByteBuffer.wrap(value, 0, length);
				buf.order(ByteOrder.LITTLE_ENDIAN);
				this.colData.set(col, buf);
				break;
			}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class NullType {
		private static final byte NULL_BOOL_VAL = 0x2;
		private static final String NULL_STR = "null";
		
		public String toString() {
			return NullType.NULL_STR;
		}

		public static boolean isBooleanNull(byte val) {
			return val == NullType.NULL_BOOL_VAL;
		}

		private static boolean isTinyIntNull(byte val) {
			return val == Byte.MIN_VALUE;
		}

		private static boolean isSmallIntNull(short val) {
			return val == Short.MIN_VALUE;
		}

		private static boolean isIntNull(int val) {
			return val == Integer.MIN_VALUE;
		}

		private static boolean isBigIntNull(long val) {
			return val == Long.MIN_VALUE;
		}

		private static boolean isFloatNull(float val) {
			return Float.isNaN(val);
		}

		private static boolean isDoubleNull(double val) {
			return Double.isNaN(val);
		}

		private static boolean isBinaryNull(byte[] val, int length) {
			if (length != Byte.BYTES) {
				return false;
			}

			return val[0] == 0xFF;
		}

		private static boolean isNcharNull(byte[] val, int length) {
			if (length != Integer.BYTES) {
				return false;
			}

			return (val[0] & val[1] & val[2] & val[3]) == 0xFF;
		}

	}

	/**
	 * The original type may not be a string type, but will be converted to by
	 * calling this method
	 * 
	 * @param col column index
	 * @return
	 * @throws SQLException
	 */
	public String getString(int col) throws SQLException {
		Object obj = get(col);
		if (obj == null) {
			return new NullType().toString();
		}

		return obj.toString();
	}

	public int getInt(int col) {
		Object obj = get(col);
		if (obj == null) {
			return 0;
		}

		int type = this.columnMetaDataList.get(col).getColType();
		switch (type) {
		case TSDBConstants.TSDB_DATA_TYPE_BOOL:
		case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
		case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
		case TSDBConstants.TSDB_DATA_TYPE_INT: {
			return (int) obj;
		}
		case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
		case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
			return ((Long) obj).intValue();
		}

		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
			return ((Double) obj).intValue();
		}

		case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
		case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
			return Integer.parseInt((String) obj);
		}
		}

		return 0;
	}

	public boolean getBoolean(int col) throws SQLException {
		Object obj = get(col);
		if (obj == null) {
			return Boolean.FALSE;
		}

		int type = this.columnMetaDataList.get(col).getColType();
		switch (type) {
		case TSDBConstants.TSDB_DATA_TYPE_BOOL:
		case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
		case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
		case TSDBConstants.TSDB_DATA_TYPE_INT: {
			return ((int) obj == 0L) ? Boolean.FALSE : Boolean.TRUE;
		}
		case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
		case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
			return (((Long) obj) == 0L) ? Boolean.FALSE : Boolean.TRUE;
		}

		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
			return (((Double) obj) == 0) ? Boolean.FALSE : Boolean.TRUE;
		}

		case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
		case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
			if ("TRUE".compareToIgnoreCase((String) obj) == 0) {
				return Boolean.TRUE;
			} else if ("FALSE".compareToIgnoreCase((String) obj) == 0) {
				return Boolean.TRUE;
			} else {
				throw new SQLDataException();
			}
		}
		}

		return Boolean.FALSE;
	}

	public long getLong(int col) throws SQLException {
		Object obj = get(col);
		if (obj == null) {
			return 0;
		}

		int type = this.columnMetaDataList.get(col).getColType();
		switch (type) {
		case TSDBConstants.TSDB_DATA_TYPE_BOOL:
		case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
		case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
		case TSDBConstants.TSDB_DATA_TYPE_INT: {
			return (int) obj;
		}
		case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
		case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
			return (long) obj;
		}

		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
			return ((Double) obj).longValue();
		}

		case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
		case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
			return Long.parseLong((String) obj);
		}
		}

		return 0;
	}

	public Timestamp getTimestamp(int col) {
		try {
			return new Timestamp(getLong(col));
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	public double getDouble(int col) {
		Object obj = get(col);
		if (obj == null) {
			return 0;
		}

		int type = this.columnMetaDataList.get(col).getColType();
		switch (type) {
		case TSDBConstants.TSDB_DATA_TYPE_BOOL:
		case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
		case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
		case TSDBConstants.TSDB_DATA_TYPE_INT: {
			return (int) obj;
		}
		case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
		case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
			return (long) obj;
		}

		case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
			return (double) obj;
		}

		case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
		case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
			return Double.parseDouble((String) obj);
		}
		}

		return 0;
	}

	public Object get(int col) {
		int fieldSize = this.columnMetaDataList.get(col).getColSize();

		switch (this.columnMetaDataList.get(col).getColType()) {
		case TSDBConstants.TSDB_DATA_TYPE_BOOL: {
			ByteBuffer bb = (ByteBuffer) this.colData.get(col);

			byte val = bb.get(this.rowIndex);
			if (NullType.isBooleanNull(val)) {
				return null;
			}

			return (val == 0x0) ? Boolean.FALSE : Boolean.TRUE;
		}

		case TSDBConstants.TSDB_DATA_TYPE_TINYINT: {
			ByteBuffer bb = (ByteBuffer) this.colData.get(col);

			byte val = bb.get(this.rowIndex);
			if (NullType.isTinyIntNull(val)) {
				return null;
			}

			return val;
		}

		case TSDBConstants.TSDB_DATA_TYPE_SMALLINT: {
			ShortBuffer sb = (ShortBuffer) this.colData.get(col);
			short val = sb.get(this.rowIndex);
			if (NullType.isSmallIntNull(val)) {
				return null;
			}

			return val;
		}

		case TSDBConstants.TSDB_DATA_TYPE_INT: {
			IntBuffer ib = (IntBuffer) this.colData.get(col);
			int val = ib.get(this.rowIndex);
			if (NullType.isIntNull(val)) {
				return null;
			}

			return val;
		}

		case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP:
		case TSDBConstants.TSDB_DATA_TYPE_BIGINT: {
			LongBuffer lb = (LongBuffer) this.colData.get(col);
			long val = lb.get(this.rowIndex);
			if (NullType.isBigIntNull(val)) {
				return null;
			}

			return (long) val;
		}

		case TSDBConstants.TSDB_DATA_TYPE_FLOAT: {
			FloatBuffer fb = (FloatBuffer) this.colData.get(col);
			float val = fb.get(this.rowIndex);
			if (NullType.isFloatNull(val)) {
				return null;
			}

			return val;
		}

		case TSDBConstants.TSDB_DATA_TYPE_DOUBLE: {
			DoubleBuffer lb = (DoubleBuffer) this.colData.get(col);
			double val = lb.get(this.rowIndex);
			if (NullType.isDoubleNull(val)) {
				return null;
			}

			return val;
		}

		case TSDBConstants.TSDB_DATA_TYPE_BINARY: {
			ByteBuffer bb = (ByteBuffer) this.colData.get(col);
			bb.position(fieldSize * this.rowIndex);

			int length = bb.getShort();

			byte[] dest = new byte[length];
			bb.get(dest, 0, length);
			if (NullType.isBinaryNull(dest, length)) {
				return null;
			}

			return new String(dest);
		}

		case TSDBConstants.TSDB_DATA_TYPE_NCHAR: {
			ByteBuffer bb = (ByteBuffer) this.colData.get(col);
			bb.position(fieldSize * this.rowIndex);

			int length = bb.getShort();

			byte[] dest = new byte[length];
			bb.get(dest, 0, length);
			if (NullType.isNcharNull(dest, length)) {
				return null;
			}

			try {
				String ss = TaosGlobalConfig.getCharset();
				return new String(dest, ss);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		}

		return 0;
	}
}
