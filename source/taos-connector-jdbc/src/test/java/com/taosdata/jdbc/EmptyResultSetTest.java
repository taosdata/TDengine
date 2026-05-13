package com.taosdata.jdbc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;

import static org.junit.Assert.*;

public class EmptyResultSetTest {

    private EmptyResultSet emptyResultSet;

    @Before
    public void setUp() {
        emptyResultSet = new EmptyResultSet();
    }

    @Test
    public void testNext() throws SQLException {
        assertFalse(emptyResultSet.next());
    }

    @Test
    public void testWasNull() throws SQLException {
        assertFalse(emptyResultSet.wasNull());
    }

    @Test
    public void testGetString() throws SQLException {
        assertNull(emptyResultSet.getString(1));
        assertNull(emptyResultSet.getString("column"));
    }

    @Test
    public void testGetBoolean() throws SQLException {
        assertFalse(emptyResultSet.getBoolean(1));
        assertFalse(emptyResultSet.getBoolean("column"));
    }

    @Test
    public void testGetByte() throws SQLException {
        assertEquals(0, emptyResultSet.getByte(1));
        assertEquals(0, emptyResultSet.getByte("column"));
    }

    @Test
    public void testGetShort() throws SQLException {
        assertEquals(0, emptyResultSet.getShort(1));
        assertEquals(0, emptyResultSet.getShort("column"));
    }

    @Test
    public void testGetInt() throws SQLException {
        assertEquals(0, emptyResultSet.getInt(1));
        assertEquals(0, emptyResultSet.getInt("column"));
    }

    @Test
    public void testGetLong() throws SQLException {
        assertEquals(0L, emptyResultSet.getLong(1));
        assertEquals(0L, emptyResultSet.getLong("column"));
    }

    @Test
    public void testGetFloat() throws SQLException {
        assertEquals(0.0f, emptyResultSet.getFloat(1), 0.0);
        assertEquals(0.0f, emptyResultSet.getFloat("column"), 0.0);
    }

    @Test
    public void testGetDouble() throws SQLException {
        assertEquals(0.0, emptyResultSet.getDouble(1), 0.0);
        assertEquals(0.0, emptyResultSet.getDouble("column"), 0.0);
    }

    @Test
    public void testGetBigDecimal() throws SQLException {
        assertNull(emptyResultSet.getBigDecimal(1));
        assertNull(emptyResultSet.getBigDecimal("column"));
    }

    @Test
    public void testGetBytes() throws SQLException {
        assertArrayEquals(new byte[0], emptyResultSet.getBytes(1));
        assertArrayEquals(new byte[0], emptyResultSet.getBytes("column"));
    }

    @Test
    public void testGetDate() throws SQLException {
        assertNull(emptyResultSet.getDate(1));
        assertNull(emptyResultSet.getDate("column"));
    }

    @Test
    public void testGetTime() throws SQLException {
        assertNull(emptyResultSet.getTime(1));
        assertNull(emptyResultSet.getTime("column"));
    }

    @Test
    public void testGetTimestamp() throws SQLException {
        assertNull(emptyResultSet.getTimestamp(1));
        assertNull(emptyResultSet.getTimestamp("column"));
    }

    @Test
    public void testGetMetaData() throws SQLException {
        assertNull(emptyResultSet.getMetaData());
    }

    @Test
    public void testGetWarnings() throws SQLException {
        assertNull(emptyResultSet.getWarnings());
    }

    @Test
    public void testGetCursorName() throws SQLException {
        assertNull(emptyResultSet.getCursorName());
    }

    @Test
    public void testGetStatement() throws SQLException {
        assertNull(emptyResultSet.getStatement());
    }

    @Test
    public void testIsClosed() throws SQLException {
        assertFalse(emptyResultSet.isClosed());
    }

    @Test
    public void testGetHoldability() throws SQLException {
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, emptyResultSet.getHoldability());
    }

    // Add more tests for other methods as needed

    @Test
    public void testGetAsciiStream() throws SQLException {
        assertNull(emptyResultSet.getAsciiStream(1));
    }

    @Test
    public void testGetUnicodeStream() throws SQLException {
        assertNull(emptyResultSet.getUnicodeStream(1));
    }

    @Test
    public void testGetBinaryStream() throws SQLException {
        assertNull(emptyResultSet.getBinaryStream(1));
    }
    @Test
    public void testGetObjectByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getObject(1));
    }

    @Test
    public void testGetObjectByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getObject("column"));
    }

    @Test
    public void testFindColumn() throws SQLException {
        assertEquals(0, emptyResultSet.findColumn("column"));
    }

    @Test
    public void testGetCharacterStreamByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getCharacterStream(1));
    }

    @Test
    public void testGetCharacterStreamByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getCharacterStream("column"));
    }

    @Test
    public void testIsBeforeFirst() throws SQLException {
        assertFalse(emptyResultSet.isBeforeFirst());
    }

    @Test
    public void testIsAfterLast() throws SQLException {
        assertFalse(emptyResultSet.isAfterLast());
    }

    @Test
    public void testIsFirst() throws SQLException {
        assertFalse(emptyResultSet.isFirst());
    }

    @Test
    public void testIsLast() throws SQLException {
        assertFalse(emptyResultSet.isLast());
    }

    @Test
    public void testBeforeFirst() throws SQLException {
        emptyResultSet.beforeFirst();
        Assert.assertNotNull(emptyResultSet);
    }

    @Test
    public void testAfterLast() throws SQLException {
        emptyResultSet.afterLast();
        Assert.assertNotNull(emptyResultSet);
    }

    @Test
    public void testFirst() throws SQLException {
        assertFalse(emptyResultSet.first());
    }

    @Test
    public void testLast() throws SQLException {
        assertFalse(emptyResultSet.last());
    }

    @Test
    public void testGetRow() throws SQLException {
        assertEquals(0, emptyResultSet.getRow());
    }

    @Test
    public void testAbsolute() throws SQLException {
        assertFalse(emptyResultSet.absolute(1));
    }

    @Test
    public void testRelative() throws SQLException {
        assertFalse(emptyResultSet.relative(1));
    }

    @Test
    public void testPrevious() throws SQLException {
        assertFalse(emptyResultSet.previous());
    }

    @Test
    public void testSetFetchDirection() throws SQLException {
        emptyResultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
        Assert.assertEquals(ResultSet.FETCH_FORWARD, emptyResultSet.getFetchDirection());
    }

    @Test
    public void testGetFetchDirection() throws SQLException {
        assertEquals(ResultSet.FETCH_FORWARD, emptyResultSet.getFetchDirection());
    }

    @Test(expected = SQLException.class)
    public void testSetFetchSize() throws SQLException {
        emptyResultSet.setFetchSize(10);
    }

    @Test
    public void testGetFetchSize() throws SQLException {
        assertEquals(0, emptyResultSet.getFetchSize());
    }

    @Test
    public void testGetType() throws SQLException {
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, emptyResultSet.getType());
    }

    @Test
    public void testGetConcurrency() throws SQLException {
        assertEquals(ResultSet.CONCUR_READ_ONLY, emptyResultSet.getConcurrency());
    }

    @Test
    public void testRowUpdated() throws SQLException {
        assertFalse(emptyResultSet.rowUpdated());
    }

    @Test
    public void testRowInserted() throws SQLException {
        assertFalse(emptyResultSet.rowInserted());
    }

    @Test
    public void testRowDeleted() throws SQLException {
        assertFalse(emptyResultSet.rowDeleted());
    }

    @Test(expected = SQLException.class)
    public void testUpdateNull() throws SQLException {
        emptyResultSet.updateNull(1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBoolean() throws SQLException {
        emptyResultSet.updateBoolean(1, true);
    }

    @Test(expected = SQLException.class)
    public void testUpdateByte() throws SQLException {
        emptyResultSet.updateByte(1, (byte) 1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateShort() throws SQLException {
        emptyResultSet.updateShort(1, (short) 1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateInt() throws SQLException {
        emptyResultSet.updateInt(1, 1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateLong() throws SQLException {
        emptyResultSet.updateLong(1, 1L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateFloatByColumnIndex() throws SQLException {
        emptyResultSet.updateFloat(1, 1.0f);
    }

    @Test(expected = SQLException.class)
    public void testUpdateDoubleByColumnIndex() throws SQLException {
        emptyResultSet.updateDouble(1, 1.0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBigDecimalByColumnIndex() throws SQLException {
        emptyResultSet.updateBigDecimal(1, new BigDecimal("1.0"));
    }

    @Test(expected = SQLException.class)
    public void testUpdateStringByColumnIndex() throws SQLException {
        emptyResultSet.updateString(1, "test");
    }

    @Test(expected = SQLException.class)
    public void testUpdateBytesByColumnIndex() throws SQLException {
        emptyResultSet.updateBytes(1, new byte[]{1, 2, 3});
    }

    @Test(expected = SQLException.class)
    public void testUpdateDateByColumnIndex() throws SQLException {
        emptyResultSet.updateDate(1, new Date(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testUpdateTimeByColumnIndex() throws SQLException {
        emptyResultSet.updateTime(1, new Time(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testUpdateTimestampByColumnIndex() throws SQLException {
        emptyResultSet.updateTimestamp(1, new Timestamp(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByColumnIndex() throws SQLException {
        emptyResultSet.updateAsciiStream(1, new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByColumnIndex() throws SQLException {
        emptyResultSet.updateBinaryStream(1, new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByColumnIndex() throws SQLException {
        emptyResultSet.updateCharacterStream(1, new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        }, 0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateObjectByColumnIndexWithScale() throws SQLException {
        emptyResultSet.updateObject(1, new Object(), 1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateObjectByColumnIndex() throws SQLException {
        emptyResultSet.updateObject(1, new Object());
    }

    @Test(expected = SQLException.class)
    public void testUpdateNullByColumnLabel() throws SQLException {
        emptyResultSet.updateNull("column");
    }

    @Test(expected = SQLException.class)
    public void testUpdateBooleanByColumnLabel() throws SQLException {
        emptyResultSet.updateBoolean("column", true);
    }

    @Test(expected = SQLException.class)
    public void testUpdateByteByColumnLabel() throws SQLException {
        emptyResultSet.updateByte("column", (byte)1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateShortByColumnLabel() throws SQLException {
        emptyResultSet.updateShort("column", (short)1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateIntByColumnLabel() throws SQLException {
        emptyResultSet.updateInt("column", 1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateLongByColumnLabel() throws SQLException {
        emptyResultSet.updateLong("column", 1L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateFloatByColumnLabel() throws SQLException {
        emptyResultSet.updateFloat("column", 1.0f);
    }

    @Test(expected = SQLException.class)
    public void testUpdateDoubleByColumnLabel() throws SQLException {
        emptyResultSet.updateDouble("column", 1.0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBigDecimalByColumnLabel() throws SQLException {
        emptyResultSet.updateBigDecimal("column", new BigDecimal("1.0"));
    }

    @Test(expected = SQLException.class)
    public void testUpdateStringByColumnLabel() throws SQLException {
        emptyResultSet.updateString("column", "test");
    }

    @Test(expected = SQLException.class)
    public void testUpdateBytesByColumnLabel() throws SQLException {
        emptyResultSet.updateBytes("column", new byte[]{1, 2, 3});
    }

    @Test(expected = SQLException.class)
    public void testUpdateDateByColumnLabel() throws SQLException {
        emptyResultSet.updateDate("column", new Date(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testUpdateTimeByColumnLabel() throws SQLException {
        emptyResultSet.updateTime("column", new Time(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testUpdateTimestampByColumnLabel() throws SQLException {
        emptyResultSet.updateTimestamp("column", new Timestamp(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByColumnLabel() throws SQLException {
        emptyResultSet.updateAsciiStream("column", new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByColumnLabel() throws SQLException {
        emptyResultSet.updateBinaryStream("column", new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByColumnLabel() throws SQLException {
        emptyResultSet.updateCharacterStream("column", new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        }, 0);
    }

    @Test(expected = SQLException.class)
    public void testUpdateObjectByColumnLabelWithScale() throws SQLException {
        emptyResultSet.updateObject("column", new Object(), 1);
    }

    @Test(expected = SQLException.class)
    public void testUpdateObjectByColumnLabel() throws SQLException {
        emptyResultSet.updateObject("column", new Object());
    }

    @Test(expected = SQLException.class)
    public void testInsertRow() throws SQLException {
        emptyResultSet.insertRow();
    }

    @Test(expected = SQLException.class)
    public void testUpdateRow() throws SQLException {
        emptyResultSet.updateRow();
    }

    @Test(expected = SQLException.class)
    public void testDeleteRow() throws SQLException {
        emptyResultSet.deleteRow();
    }

    @Test(expected = SQLException.class)
    public void testRefreshRow() throws SQLException {
        emptyResultSet.refreshRow();
    }

    @Test(expected = SQLException.class)
    public void testCancelRowUpdates() throws SQLException {
        emptyResultSet.cancelRowUpdates();
    }

    @Test(expected = SQLException.class)
    public void testMoveToInsertRow() throws SQLException {
        emptyResultSet.moveToInsertRow();
    }

    @Test(expected = SQLException.class)
    public void testMoveToCurrentRow() throws SQLException {
        emptyResultSet.moveToCurrentRow();
    }

    @Test
    public void testGetObjectByColumnIndexWithMap() throws SQLException {
        assertNull(emptyResultSet.getObject(1, new HashMap<>()));
    }

    @Test
    public void testGetRefByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getRef(1));
    }

    @Test
    public void testGetBlobByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getBlob(1));
    }

    @Test
    public void testGetClobByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getClob(1));
    }

    @Test
    public void testGetArrayByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getArray(1));
    }

    @Test
    public void testGetObjectByColumnLabelWithMap() throws SQLException {
        assertNull(emptyResultSet.getObject("column", new HashMap<>()));
    }

    @Test
    public void testGetRefByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getRef("column"));
    }

    @Test
    public void testGetBlobByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getBlob("column"));
    }

    @Test
    public void testGetClobByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getClob("column"));
    }

    @Test
    public void testGetArrayByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getArray("column"));
    }

    @Test
    public void testGetDateByColumnIndexWithCalendar() throws SQLException {
        assertNull(emptyResultSet.getDate(1, Calendar.getInstance()));
    }

    @Test
    public void testGetDateByColumnLabelWithCalendar() throws SQLException {
        assertNull(emptyResultSet.getDate("column", Calendar.getInstance()));
    }

    @Test
    public void testGetTimeByColumnIndexWithCalendar() throws SQLException {
        assertNull(emptyResultSet.getTime(1, Calendar.getInstance()));
    }

    @Test
    public void testGetTimeByColumnLabelWithCalendar() throws SQLException {
        assertNull(emptyResultSet.getTime("column", Calendar.getInstance()));
    }

    @Test
    public void testGetTimestampByColumnIndexWithCalendar() throws SQLException {
        assertNull(emptyResultSet.getTimestamp(1, Calendar.getInstance()));
    }
    @Test
    public void testGetTimestampByColumnLabelWithCalendar() throws SQLException {
        assertNull(emptyResultSet.getTimestamp("column", Calendar.getInstance()));
    }

    @Test
    public void testGetURLByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getURL(1));
    }

    @Test
    public void testGetURLByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getURL("column"));
    }

    @Test(expected = SQLException.class)
    public void testUpdateRefByColumnIndex() throws SQLException {
        emptyResultSet.updateRef(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateRefByColumnLabel() throws SQLException {
        emptyResultSet.updateRef("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByColumnIndex() throws SQLException {
        emptyResultSet.updateBlob(1, (Blob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByColumnLabel() throws SQLException {
        emptyResultSet.updateBlob("column", (Blob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByColumnIndex() throws SQLException {
        emptyResultSet.updateClob(1, (Clob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByColumnLabel() throws SQLException {
        emptyResultSet.updateClob("column", (Clob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateArrayByColumnIndex() throws SQLException {
        emptyResultSet.updateArray(1, (Array) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateArrayByColumnLabel() throws SQLException {
        emptyResultSet.updateArray("column", (Array) null);
    }

    @Test
    public void testGetRowIdByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getRowId(1));
    }

    @Test
    public void testGetRowIdByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getRowId("column"));
    }

    @Test(expected = SQLException.class)
    public void testUpdateRowIdByColumnIndex() throws SQLException {
        emptyResultSet.updateRowId(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateRowIdByColumnLabel() throws SQLException {
        emptyResultSet.updateRowId("column", null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNStringByColumnIndex() throws SQLException {
        emptyResultSet.updateNString(1, "test");
    }

    @Test(expected = SQLException.class)
    public void testUpdateNStringByColumnLabel() throws SQLException {
        emptyResultSet.updateNString("column", "test");
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByColumnIndex() throws SQLException {
        emptyResultSet.updateNClob(1, (NClob) null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByColumnLabel() throws SQLException {
        emptyResultSet.updateNClob("column", (NClob) null);
    }

    @Test
    public void testGetNClobByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getNClob(1));
    }

    @Test
    public void testGetNClobByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getNClob("column"));
    }

    @Test
    public void testGetSQLXMLByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getSQLXML(1));
    }

    @Test
    public void testGetSQLXMLByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getSQLXML("column"));
    }

    @Test(expected = SQLException.class)
    public void testUpdateSQLXMLByColumnIndex() throws SQLException {
        emptyResultSet.updateSQLXML(1, null);
    }

    @Test(expected = SQLException.class)
    public void testUpdateSQLXMLByColumnLabel() throws SQLException {
        emptyResultSet.updateSQLXML("column", null);
    }

    @Test
    public void testGetNStringByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getNString(1));
    }

    @Test
    public void testGetNStringByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getNString("column"));
    }

    @Test
    public void testGetNCharacterStreamByColumnIndex() throws SQLException {
        assertNull(emptyResultSet.getNCharacterStream(1));
    }
    @Test
    public void testGetNCharacterStreamByColumnLabel() throws SQLException {
        assertNull(emptyResultSet.getNCharacterStream("column"));
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByColumnIndexWithLength() throws SQLException {
        emptyResultSet.updateNCharacterStream(1, new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByColumnLabelWithLength() throws SQLException {
        emptyResultSet.updateNCharacterStream("column", new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByColumnIndexWithLength() throws SQLException {
        emptyResultSet.updateAsciiStream(1, new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByColumnIndexWithLength() throws SQLException {
        emptyResultSet.updateBinaryStream(1, new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByColumnIndexWithLength() throws SQLException {
        emptyResultSet.updateCharacterStream(1, new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateAsciiStreamByColumnLabelWithLength() throws SQLException {
        emptyResultSet.updateAsciiStream("column", new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBinaryStreamByColumnLabelWithLength() throws SQLException {
        emptyResultSet.updateBinaryStream("column", new InputStream() {
            @Override
            public int read() { return -1; }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateCharacterStreamByColumnLabelWithLength() throws SQLException {
        emptyResultSet.updateCharacterStream("column", new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        }, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByColumnIndexWithInputStreamAndLength() throws SQLException {
        emptyResultSet.updateBlob(1, null, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByColumnLabelWithInputStreamAndLength() throws SQLException {
        emptyResultSet.updateBlob("column", null, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByColumnIndexWithReaderAndLength() throws SQLException {
        emptyResultSet.updateClob(1, null, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByColumnLabelWithReaderAndLength() throws SQLException {
        emptyResultSet.updateClob("column", null, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByColumnIndexWithReaderAndLength() throws SQLException {
        emptyResultSet.updateNClob(1, null, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByColumnLabelWithReaderAndLength() throws SQLException {
        emptyResultSet.updateNClob("column", null, 0L);
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByColumnIndex() throws SQLException {
        emptyResultSet.updateNCharacterStream(1, new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testUpdateNCharacterStreamByColumnLabel() throws SQLException {
        emptyResultSet.updateNCharacterStream("column", new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByColumnIndexWithInputStream() throws SQLException {
        emptyResultSet.updateBlob(1, new InputStream() {
            @Override
            public int read() { return -1; }
        });
    }

    @Test(expected = SQLException.class)
    public void testUpdateBlobByColumnLabelWithInputStream() throws SQLException {
        emptyResultSet.updateBlob("column", new InputStream() {
            @Override
            public int read() { return -1; }
        });
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByColumnIndexWithReader() throws SQLException {
        emptyResultSet.updateClob(1, new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testUpdateClobByColumnLabelWithReader() throws SQLException {
        emptyResultSet.updateClob("column", new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByColumnIndexWithReader() throws SQLException {
        emptyResultSet.updateNClob(1, new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testUpdateNClobByColumnLabelWithReader() throws SQLException {
        emptyResultSet.updateNClob("column", new Reader() {
            @Override
            public int read(char[] cbuf, int off, int len) { return -1; }
            @Override
            public void close() {
                // do nothing
            }
        });
    }

    @Test
    public void testGetObjectByColumnIndexWithType() throws SQLException {
        assertNull(emptyResultSet.getObject(1, Object.class));
    }

    @Test
    public void testGetObjectByColumnLabelWithType() throws SQLException {
        assertNull(emptyResultSet.getObject("column", Object.class));
    }

    @Test
    public void testUnwrap() throws SQLException {
        assertNull(emptyResultSet.unwrap(Object.class));
    }

    @Test
    public void testIsWrapperFor() throws SQLException {
        assertFalse(emptyResultSet.isWrapperFor(Object.class));
    }

    @Test
    public void testGetBigDecimalByColumnLabelWithScale() throws SQLException {
        assertNull(emptyResultSet.getBigDecimal("column", 1));
    }

    @Test
    public void testGetBigDecimalByColumnIndexWithScale() throws SQLException {
        assertNull(emptyResultSet.getBigDecimal(1, 1));
    }

    @Test
    public void testClose() throws SQLException {
        emptyResultSet.close();
    }
}

