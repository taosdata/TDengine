package com.taosdata.iot.clients;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBResultSetMetaData;
import com.taosdata.jdbc.TSDBResultSetRowData;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * A container for polling result sets. ConsumerResultSet is a subclass of java.sql.ResultSet and provides the most
 * commonly used data access methods following the JDBC standard.
 */
public class ConsumerResultSet implements ResultSet {

    private ArrayList<TSDBResultSetRowData> rowDataList;
    private TSDBResultSetMetaData resultSetMetaData;

    private int cursorPos = 0;
    private boolean lastWasNull = false;
    private boolean isClosed = false;

    /**
     * Get all records from a poll operation
     * @return
     */
    public List<TSDBResultSetRowData> getRows() {
        return rowDataList;
    }

    /**
     * Get the metadata for the retrieved result set
     */
    public TSDBResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    public void setResultSetMetaData(TSDBResultSetMetaData resultSetMetaData) {
        this.resultSetMetaData = resultSetMetaData;
    }

    /**
     * Add a new record in the result set
     * @param rowData
     */
    public void addRow(TSDBResultSetRowData rowData) {
        if (rowData != null) {
            if (rowDataList == null) {
                rowDataList = new ArrayList<>();
            }
            TSDBResultSetRowData rowData1 = new TSDBResultSetRowData(rowData.getColSize());
            rowData1.setData(rowData.getData());
            rowDataList.add(rowData1);
        }
    }

    /**
     * Retrieve a single record from the result set by index
     * @param index index of the record
     * @return
     */
    public TSDBResultSetRowData getRow(int index){
        if (rowDataList == null) {
            return null;
        } else {
            return rowDataList.get(index);
        }
    }

    /**
     * Get the total number of records in this result set
     * @return
     */
    public int size() {
        if (rowDataList == null) {
            return 0;
        } else {
            return rowDataList.size();
        }
    }


    @Override
    public boolean next() throws SQLException {
        if (rowDataList == null || rowDataList.size() == 0) {
            // no data in result set
            return false;
        } else {
            if (cursorPos < rowDataList.size()) {
                cursorPos++;
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void close() throws SQLException {
        rowDataList = null;
        resultSetMetaData = null;
        cursorPos = 0;
        isClosed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastWasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        String res = null;
        int colIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(colIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getString(colIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        boolean res = false;
        int trueColIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(trueColIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getBoolean(trueColIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        byte res = 0;
        int trueColIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(trueColIndex);
        if (!lastWasNull) {
            res = (byte) rowDataList.get(cursorPos - 1).getInt(trueColIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        short res = 0;
        int trueColIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(trueColIndex);
        if (!lastWasNull) {
            res = (short) rowDataList.get(cursorPos - 1).getInt(trueColIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        int res = 0;
        int trueColIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(trueColIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getInt(trueColIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        long res = 0L;
        int trueColIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(trueColIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getLong(trueColIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        float res = 0;
        int trueColIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(trueColIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getFloat(trueColIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        double res = 0;
        int trueColIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(trueColIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getDouble(trueColIndex, resultSetMetaData.getColumnType(columnIndex));
        }
        return res;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal res = null;
        int colIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(colIndex);
        if (!lastWasNull) {
            res = new BigDecimal(rowDataList.get(cursorPos - 1).getLong(colIndex, resultSetMetaData.getColumnType(columnIndex)));
        }
        return res;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        byte[] res = null;
        int colIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(colIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getString(colIndex, resultSetMetaData.getColumnType(columnIndex)).getBytes();
        }
        return res;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        int colIndex = getTrueColumnIndex(columnIndex);
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Timestamp res = null;
        int colIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(colIndex);
        if (!lastWasNull) {
            res = rowDataList.get(cursorPos - 1).getTimestamp(colIndex);
        }
        return res;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return this.getString(this.findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return this.getBoolean(this.findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return this.getByte(this.findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return this.getShort(this.findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return this.getInt(this.findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return this.getLong(this.findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return this.getFloat(this.findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return this.getDouble(this.findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return this.getBytes(this.findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return this.getTimestamp(this.findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        int colIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(colIndex);
        return rowDataList.get(cursorPos - 1).get(colIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return this.getObject(this.findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {

        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
            if(resultSetMetaData.getColumnLabel(i).equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException(TSDBConstants.INVALID_VARIABLES);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        int colIndex = getTrueColumnIndex(columnIndex);

        this.lastWasNull = rowDataList.get(cursorPos - 1).wasNull(colIndex);
        return new BigDecimal(rowDataList.get(cursorPos - 1).getLong(colIndex, resultSetMetaData.getColumnType(colIndex)));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        return cursorPos < 1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        if (rowDataList != null && rowDataList.size() > 0) {
            return cursorPos > rowDataList.size();
        } else {
            return false;
        }
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        return cursorPos == 1;
    }

    public boolean isLast() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        return cursorPos == rowDataList.size();
    }

    public void beforeFirst() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        if (rowDataList != null && rowDataList.size() > 0) {
            cursorPos = 0;
        }
    }

    public void afterLast() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        if (rowDataList != null && rowDataList.size() > 0) {
            cursorPos = rowDataList.size() + 1;
        }
    }

    public boolean first() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        if (rowDataList != null && rowDataList.size() > 0) {
            if (cursorPos > 0 && cursorPos <= rowDataList.size()) {
                cursorPos = 1;
                return true;
            }
        }
        return false;
    }

    public boolean last() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        if (rowDataList != null && rowDataList.size() > 0) {
            if (cursorPos > 0 && cursorPos <= rowDataList.size()) {
                cursorPos = rowDataList.size();
                return true;
            }
        }
        return false;
    }

    public int getRow() throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        if (rowDataList != null && rowDataList.size() > 0) {
            if (cursorPos > 0 && cursorPos <= rowDataList.size()) {
                return cursorPos;
            }
        }
        return 0;
    }

    public boolean absolute(int row) throws SQLException {
        if (isClosed) {
            throw new SQLException("Can not access cursor on a closed result set!");
        }
        if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("The resut set is of type forward-only!");
        }
        boolean inRange = false;
        if (rowDataList != null && rowDataList.size() > 0) {
            if (cursorPos >= 1 && cursorPos <= rowDataList.size()) {
                // cursor is before the first or after the last
                if (row == 0) {
                    cursorPos = 0;
                } else if (row > 0) {
                    cursorPos = row;
                    if (cursorPos > rowDataList.size()) {
                        cursorPos = -1;
                    } else {
                        inRange = true;
                    }
                } else {
                    cursorPos = rowDataList.size() + 1 + row;
                    if (cursorPos < 0) {
                        cursorPos = 0;
                    } else {
                        inRange = true;
                    }
                }
            }

        }
        return inRange;
    }

    public boolean relative(int rows) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public boolean previous() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public int getFetchDirection() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public int getFetchSize() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getConcurrency() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public boolean rowUpdated() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public boolean rowInserted() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public boolean rowDeleted() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void insertRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void deleteRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void refreshRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Statement getStatement() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public int getHoldability() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public String getNString(int columnIndex) throws SQLException {
        int colIndex = getTrueColumnIndex(columnIndex);
        return (String) rowDataList.get(cursorPos).get(colIndex);
    }

    public String getNString(String columnLabel) throws SQLException {
        return (String) this.getString(columnLabel);
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    private int getTrueColumnIndex(int columnIndex) throws SQLException {
        if (cursorPos < 0) {
            throw new SQLException("Cursor is not positioned");
        }
        if (columnIndex < 1) {
            // column index should start from 1
            throw new SQLException("Column Index out of range, " + columnIndex + " < 1");
        }

        int numOfCols = resultSetMetaData.getColumnCount();
        if (columnIndex > numOfCols) {
            throw new SQLException("Column Index out of range, " + columnIndex + " > " + numOfCols);
        }

        return columnIndex - 1;
    }
}
