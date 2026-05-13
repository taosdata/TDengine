package com.taosdata.jdbc.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class TDBlobTest {

    private TDBlob writableBlob;
    private TDBlob readOnlyBlob;
    private final byte[] testData = {1, 2, 3, 4, 5};

    @Before
    public void setUp() {
        writableBlob = new TDBlob(testData.clone(), false);
        readOnlyBlob = new TDBlob(testData.clone(), true);
    }

    @After
    public void tearDown() throws SQLException {
        if (writableBlob != null) writableBlob.free();
        if (readOnlyBlob != null) readOnlyBlob.free();
    }

    @Test
    public void testLength() throws SQLException {
        assertEquals(5, writableBlob.length());
    }

    @Test(expected = SQLException.class)
    public void testLengthAfterFree() throws SQLException {
        writableBlob.free();
        writableBlob.length();
    }

    @Test
    public void testGetBytes() throws SQLException {
        // normal range
        byte[] result = writableBlob.getBytes(1, 3);
        assertArrayEquals(new byte[]{1, 2, 3}, result);

        // length out range
        result = writableBlob.getBytes(3, 10);
        assertArrayEquals(new byte[]{3, 4, 5}, result);

        // edge test
        result = writableBlob.getBytes(5, 1);
        assertArrayEquals(new byte[]{5}, result);
    }

    @Test(expected = SQLException.class)
    public void testGetBytesPositionTooLow() throws SQLException {
        writableBlob.getBytes(0, 1);
    }

    @Test(expected = SQLException.class)
    public void testGetBytesPositionTooHigh() throws SQLException {
        writableBlob.getBytes(6, 1);
    }

    @Test
    public void testGetBinaryStream() throws SQLException, IOException {
        InputStream is = writableBlob.getBinaryStream();
        assertEquals(1, is.read());
        assertEquals(2, is.read());
        is.close();
    }

    @Test
    public void testPositionBlobPattern() throws SQLException {
        Blob pattern = new TDBlob(new byte[]{3, 4}, false);
        assertEquals(3, writableBlob.position(pattern, 1));
        assertEquals(-1, readOnlyBlob.position(pattern, 0));
        assertEquals(-1, readOnlyBlob.position(pattern, 6));

        // could not find
        pattern = new TDBlob(new byte[]{6, 7}, false);
        assertEquals(-1, writableBlob.position(pattern, 1));

        // empty data
        pattern = new TDBlob(new byte[]{}, false);
        assertEquals(1, writableBlob.position(pattern, 1));
    }

    @Test
    public void testPositionByteArrayPattern() throws SQLException {
        // can be find
        assertEquals(2, writableBlob.position(new byte[]{2, 3}, 1));

        // find from position
        assertEquals(4, writableBlob.position(new byte[]{4, 5}, 2));

        // could not find
        assertEquals(-1, writableBlob.position(new byte[]{6, 7}, 1));

        // empty data
        assertEquals(1, writableBlob.position(new byte[]{}, 1));

        // Boyer-Moore test
        byte[] longData = "ABCABDABACDABABCABAB".getBytes();
        byte[] pattern = "ABABCABAB".getBytes();
        TDBlob longBlob = new TDBlob(longData, true);
        assertEquals(12, longBlob.position(pattern, 1));
    }

    @Test
    public void testSetBytes() throws SQLException {
        int result = writableBlob.setBytes(1, new byte[]{9, 8, 7});
        assertEquals(3, result);
        assertArrayEquals(new byte[]{9, 8, 7}, writableBlob.getBytes(1, 3));
    }

    @Test(expected = SQLException.class)
    public void testSetBytesOnReadOnly() throws SQLException {
        readOnlyBlob.setBytes(1, new byte[]{9, 8, 7});
    }

    @Test(expected = SQLException.class)
    public void testSetBytesInvalidPosition() throws SQLException {
        readOnlyBlob.setBytes(2, new byte[]{9, 8, 7});
    }

    @Test
    public void testSetBytesWithOffset() throws SQLException {
        int result = writableBlob.setBytes(1, new byte[]{9, 8, 7, 6}, 1, 2);
        assertEquals(2, result);
        assertArrayEquals(new byte[]{8, 7}, writableBlob.getBytes(1, 20));
    }

    @Test(expected = SQLException.class)
    public void testSetBytesWithInvalidOffset() throws SQLException {
        writableBlob.setBytes(1, new byte[]{1, 2}, 1, 2);
    }

    @Test(expected = SQLException.class)
    public void testTruncate() throws SQLException {
        writableBlob.truncate(2);
    }

    @Test
    public void testFree() throws SQLException {
        writableBlob.free();
        writableBlob.free();
    }

    @Test
    public void testGetBinaryStreamWithRange() throws SQLException, IOException {
        InputStream is = writableBlob.getBinaryStream(2, 3);
        assertEquals(2, is.read());
        assertEquals(3, is.read());
        assertEquals(4, is.read());
        assertEquals(-1, is.read()); // 超出范围
        is.close();
    }

    @Test(expected = SQLException.class)
    public void testGetBinaryStreamInvalidRange() throws SQLException {
        writableBlob.getBinaryStream(0, 1);
    }

    @Test
    public void testSetBinaryStream() throws SQLException, IOException {
        OutputStream os = writableBlob.setBinaryStream(1);
        os.write(new byte[]{10, 20, 30});
        os.close();

        assertArrayEquals(new byte[]{10, 20, 30}, writableBlob.getBytes(1, 3));
    }

    @Test(expected = SQLException.class)
    public void testSetBinaryStreamOnReadOnly() throws SQLException {
        readOnlyBlob.setBinaryStream(1);
    }

    @Test(expected = SQLException.class)
    public void testSetBinaryStreamInvalidPosition() throws SQLException {
        writableBlob.setBinaryStream(2);
    }

    @Test
    public void testTDOutputStream() throws IOException, SQLException {
        OutputStream os = writableBlob.setBinaryStream(1);
        os.write(1);
        os.write(2);
        os.write(3);
        os.close();

        assertArrayEquals(new byte[]{1, 2, 3}, writableBlob.getBytes(1, 3));
    }
}