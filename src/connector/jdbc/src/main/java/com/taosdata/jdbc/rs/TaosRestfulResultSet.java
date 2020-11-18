package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBConstants;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

public class TaosRestfulResultSet implements ResultSet {
    private boolean isClosed = false;
    private int pos = -1;
    private ArrayList<ArrayList<String>> data;
    private ArrayList<String> fields;

    public TaosRestfulResultSet(String str, String fieldData) {
        data = new ArrayList<>();
        str = str.substring(2, str.length() - 2);
        ArrayList<String> strTemp = new ArrayList<>(Arrays.asList(str.split("],\\[")));
        for (String s : strTemp) {
            ArrayList<String> curr = new ArrayList<>(Arrays.asList(s.split(",")));
            data.add(curr);
        }
        if (!StringUtils.isBlank(fieldData)) {
            fields = new ArrayList<>();
            fieldData = fieldData.substring(2, fieldData.length() - 2);
            ArrayList<String> fieldTemp = new ArrayList<>(Arrays.asList(fieldData.split("],\\[")));
            for (String s : fieldTemp) {
                String curr = Arrays.asList(s.split(",")).get(0);
                fields.add(curr.substring(1, curr.length() - 1)); // 去掉双引号
            }
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed) throw new SQLException(TSDBConstants.WrapErrMsg("Result is Closed!!!"));
        if (pos < data.size() - 1) {
            pos++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return data.isEmpty();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (columnIndex > data.get(pos).size()) {
            throw new SQLException(TSDBConstants.WrapErrMsg("Column Index out of range, " + columnIndex + " > " + data.get(pos).size()));
        }
        return data.get(pos).get(columnIndex - 1);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String result = getString(columnIndex);
        if (!(result.equals("true") || result.equals("false"))) {
            throw new SQLException("not boolean value");
        }
        return result.equals("true");
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return Short.parseShort(getString(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String result = getString(columnIndex);
        return Integer.parseInt(result);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String result = getString(columnIndex);
        return Long.parseLong(result);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        String result = getString(columnIndex);
        return Float.parseFloat(result);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String result = getString(columnIndex);
        return Double.parseDouble(result);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String strDate = getString(columnIndex);
        strDate = strDate.substring(1, strDate.length() - 1);
        return Timestamp.valueOf(strDate);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel) + 1);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return Boolean.parseBoolean(getString(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return Short.parseShort(getString(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return Integer.parseInt(getString(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return Long.parseLong(getString(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        String result = getString(columnLabel);
        return Float.parseFloat(result);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return Double.parseDouble(getString(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return Timestamp.valueOf(getString(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
        //TODO: SQLFeature Not Supported
//        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        return;
        //TODO: SQLFeature Not Supported
//        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCursorName() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new TaosRestfulResultSetMetaData(fields);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
//        return null;
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
//        return null;
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return fields.indexOf(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isLast() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean first() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean last() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getType() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getConcurrency() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
//        return 0;
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
        //TODO: SQLFeature Not Supported
//        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        //TODO: SQLFeature Not Supported
        throw new SQLFeatureNotSupportedException();
    }
}
