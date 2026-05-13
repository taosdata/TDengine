package com.taosdata.jdbc.common;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class AutoExpandingBufferTest {
    private static final int INITIAL_BUFFER_SIZE = 16;  // Initial buffer size (example value)
    private static final int MAX_COMPONENTS = 30;        // Maximum number of components (example value)
    private AutoExpandingBuffer buffer;

    // Initialize test resources
    @Before
    public void setUp() {
        buffer = new AutoExpandingBuffer(INITIAL_BUFFER_SIZE, MAX_COMPONENTS);
    }

    // Clean up test resources
    @After
    public void tearDown() {
        buffer.release();  // Ensure all buffer references are released
    }

    @Test
    public void writeBytes_SingleBuffer() throws SQLException {
        // Test: Write directly to the current buffer when data fits within a single buffer
        byte[] data = "test".getBytes(StandardCharsets.UTF_8); // 4 bytes < 16 bytes
        buffer.writeBytes(data);

        buffer.stopWrite();
        CompositeByteBuf composite = buffer.getBuffer();

        assertEquals(1, composite.numComponents());  // Only one component
        assertArrayEquals(data, getCompositeContent(composite));
    }

    @Test
    public void writeBytes_MultiBufferExpansion() throws SQLException {
        // Test: Automatically expand to new components when data exceeds single buffer size
        byte[] data = new byte[INITIAL_BUFFER_SIZE * 2 + 5];  // 37 bytes (16*2+5)
        Arrays.fill(data, (byte) 0xAA);

        buffer.writeBytes(data);
        buffer.stopWrite();
        CompositeByteBuf composite = buffer.getBuffer();

        assertEquals(3, composite.numComponents());  // Initial buffer + 2 expansions
        assertArrayEquals(data, getCompositeContent(composite));
    }

    @Test(expected = SQLException.class)
    public void writeBytes_ExceedMaxComponents() throws SQLException {
        AutoExpandingBuffer smallBuffer = new AutoExpandingBuffer(4, 2);  // Max 2 components
        byte[] data = new byte[12];  // Requires 3 components (4+4+4)

        try {
            smallBuffer.writeBytes(data);  // Should trigger exception
        } finally {
            smallBuffer.release();  // Ensure resources are released
        }
    }

    // ====================================== writeString Tests ======================================
    @Test
    public void writeString_UTF8Encoding() throws SQLException {
        String str = "中文测试";
        byte[] expected = str.getBytes(StandardCharsets.UTF_8);

        buffer.writeString(str);
        buffer.stopWrite();

        assertArrayEquals(expected, getCompositeContent(buffer.getBuffer()));
    }

    @Test
    public void writeString_BufferExpansion() throws SQLException {
        // Test: Long string triggers buffer expansion
        String longStr = repeatString("a", INITIAL_BUFFER_SIZE * 2 + 10);  // Exceeds 2 buffer sizes
        buffer.writeString(longStr);
        buffer.stopWrite();

        CompositeByteBuf composite = buffer.getBuffer();
        assertTrue(composite.numComponents() > 1);  // At least one expansion
        assertArrayEquals(longStr.getBytes(StandardCharsets.UTF_8), getCompositeContent(composite));
    }

    // ====================================== Numeric Type Write Tests ======================================
    @Test
    public void writeInt_SufficientSpace() throws SQLException {
        // Test: Write directly when current buffer has sufficient space
        int value = 0x12345678;
        buffer.writeInt(value);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // Verify little-endian byte order (LE): 78 56 34 12
        assertEquals(0x78, content[0] & 0xFF);
        assertEquals(0x56, content[1] & 0xFF);
        assertEquals(0x34, content[2] & 0xFF);
        assertEquals(0x12, content[3] & 0xFF);
    }

    @Test
    public void writeInt_InsufficientSpace() throws SQLException {
        // Test: Write via heap buffer when current buffer has insufficient space
        buffer.writeBytes(new byte[INITIAL_BUFFER_SIZE - 2]);  // Leave only 2 bytes (insufficient for 4-byte int)
        int value = 0x9ABCDEF0;
        buffer.writeInt(value);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // First 14 bytes are pre-written zeros, last 4 bytes are LE representation of value
        byte[] expected = new byte[INITIAL_BUFFER_SIZE - 2 + 4];
        Arrays.fill(expected, (byte) 0);
        expected[INITIAL_BUFFER_SIZE - 2] = (byte) 0xF0;
        expected[INITIAL_BUFFER_SIZE - 1] = (byte) 0xDE;
        expected[INITIAL_BUFFER_SIZE] = (byte) 0xBC;
        expected[INITIAL_BUFFER_SIZE + 1] = (byte) 0x9A;

        assertArrayEquals(expected, content);
    }

    // ====================================== Serialization Method Tests ======================================
    @Test
    public void serializeLong_NormalCase() throws SQLException {
        long value = 1234567890L;
        buffer.serializeLong(value, false, (byte) 8);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // Verify fixed 26-byte structure (LE byte order)
        assertEquals(26, content.length);
        assertLEInt(content, 0, 26);   // totalLen=26
        assertLEInt(content, 4, 8);    // type=8
        assertLEInt(content, 8, 1);    // count=1
        assertEquals(0, content[12]);   // isNull=0
        assertEquals(0, content[13]);   // isNull=0
        assertLEInt(content, 14, 8);   // dataLength=8
        assertLELong(content, 18, value); // data
    }

    @Test
    public void serializeDouble_NullValue() throws SQLException {
        // Test: Serialization logic for null values
        buffer.serializeDouble(0, true);  // isNull=true

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // Verify isNull flag
        assertEquals(1, content[12] & 0xFF);  // isNull=1
        assertLEDouble(content, 18, 0);       // Null value writes 0
    }

    @Test
    public void serializeBool_NullValue() throws SQLException {
        // Test: Serialization logic for null values
        buffer.serializeBool(false, true);  // isNull=true

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // Verify isNull flag
        assertEquals(1, content[12] & 0xFF);  // isNull=1
        assertEquals(0, content[18]);       // Null value writes 0
    }

    @Test
    public void serializeByte_NullValue() throws SQLException {
        // Test: Serialization logic for null values
        buffer.serializeByte((byte)0, true, (byte)2);  // isNull=true

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // Verify isNull flag
        assertEquals(1, content[12] & 0xFF);  // isNull=1
        assertEquals(0, content[18]);       // Null value writes 0
    }

    // ====================================== Edge Case Tests ======================================
    @Test
    public void edgeCase_BufferExactlyFull() throws SQLException {
        // Test: Trigger expansion when buffer is exactly full
        byte[] fullData = new byte[INITIAL_BUFFER_SIZE];
        Arrays.fill(fullData, (byte) 0x55);

        buffer.writeBytes(fullData);  // Fill current buffer
        buffer.writeBytes(new byte[1]);  // Trigger expansion

        buffer.stopWrite();
        assertEquals(2, buffer.getBuffer().numComponents());  // Original buffer + new buffer
    }

    // ====================================== Resource Management Tests ======================================
    @Test
    public void release_ShouldDeallocate() throws SQLException {
        // Test: Ensure release method correctly deallocates buffer
        buffer.writeBytes(new byte[10]);
        buffer.stopWrite();

        CompositeByteBuf composite = buffer.getBuffer();
        int refCntBefore = composite.refCnt();
        buffer.release();

        assertEquals(0, composite.refCnt());  // Reference count should be zero
        assertNull(buffer.getBuffer());       // Composite should be set to null
    }

    @Test
    public void stopWrite_ShouldTransferOwnership() throws SQLException {
        // Test: Current buffer is added to Composite after stopWrite
        buffer.writeBytes(new byte[5]);
        buffer.stopWrite();

        CompositeByteBuf composite = buffer.getBuffer();
        assertEquals(1, composite.numComponents());
    }

    // ====================================== New Basic Method Tests ======================================
    @Test
    public void writeShort_SufficientSpace() throws SQLException {
        short value = 0x1234;
        buffer.writeShort(value);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // Verify little-endian byte order (LE): 34 12
        assertEquals(0x34, content[0] & 0xFF);
        assertEquals(0x12, content[1] & 0xFF);
    }

    @Test
    public void writeShort_InsufficientSpace() throws SQLException {
        buffer.writeBytes(new byte[INITIAL_BUFFER_SIZE - 1]);  // Leave only 1 byte (insufficient for 2-byte short)
        short value = (short)0x9ABC;
        buffer.writeShort(value);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        // First 15 bytes are pre-written zeros, last 2 bytes are LE representation of value
        byte[] expected = new byte[INITIAL_BUFFER_SIZE - 1 + 2];
        Arrays.fill(expected, (byte) 0);
        expected[INITIAL_BUFFER_SIZE - 1] = (byte) 0xBC;
        expected[INITIAL_BUFFER_SIZE] = (byte) 0x9A;

        assertArrayEquals(expected, content);
    }

    @Test
    public void testGetBuffer() {
        assertNotNull(buffer.getBuffer());
        assertTrue(buffer.getBuffer() instanceof CompositeByteBuf);
    }

    // ====================================== Serialization Method Tests ======================================
    @Test
    public void serializeTimeStamp_NormalCase() throws SQLException {
        long value = 1638451200000L; // 2021-12-03T00:00:00Z
        buffer.serializeTimeStamp(value, false);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        assertEquals(26, content.length);
        assertLEInt(content, 0, 26);   // totalLen=26
        assertLEInt(content, 4, 9);    // type=9 (TIMESTAMP)
        assertLEInt(content, 8, 1);    // count=1
        assertEquals(0, content[12]);   // isNull=0
        assertLEInt(content, 14, 8);   // dataLength=8
        assertLELong(content, 18, value); // timestamp value
    }

    @Test
    public void serializeTimeStamp_NullValue() throws SQLException {
        buffer.serializeTimeStamp(0, true);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        assertEquals(1, content[12] & 0xFF);  // isNull=1
        assertLELong(content, 18, 0);         // Null value writes 0
    }

    @Test
    public void serializeFloat_NormalCase() throws SQLException {
        float value = 3.14159f;
        buffer.serializeFloat(value, false);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        assertEquals(22, content.length);
        assertLEInt(content, 0, 22);   // totalLen=22
        assertLEInt(content, 4, 6);    // type=6 (FLOAT)
        assertLEInt(content, 8, 1);    // count=1
        assertEquals(0, content[12]);   // isNull=0
        assertLEInt(content, 14, 4);   // dataLength=4
        assertEquals(value, Float.intBitsToFloat(getLEInt(content, 18)), 0.0001f);
    }

    @Test
    public void serializeInt_NormalCase() throws SQLException {
        int value = 12345678;
        buffer.serializeInt(value, false, (byte)4);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        assertEquals(22, content.length);
        assertLEInt(content, 0, 22);
        assertLEInt(content, 4, 4);
        assertLEInt(content, 8, 1);    // count=1
        assertEquals(0, content[12]);   // isNull=0
        assertLEInt(content, 14, 4);   // dataLength=4
        assertLEInt(content, 18, 12345678);
    }

    @Test
    public void serializeShort_NormalCase() throws SQLException {
        short value = 12345;
        buffer.serializeShort(value, false, (byte)3);

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        assertEquals(20, content.length);
        assertLEInt(content, 0, 20);
        assertLEInt(content, 4, 3);
        assertLEInt(content, 8, 1);    // count=1
        assertEquals(0, content[12]);   // isNull=0
        assertLEInt(content, 14, 2);   // dataLength=2
        assertLEShort(content, 18, (short)12345);
    }

    @Test
    public void serializeBytes_NormalCase() throws SQLException {
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        buffer.serializeBytes(data, false, 12); // type=12 (BINARY)

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        assertEquals(22 + 4, content.length);  // Header 22 bytes + Data 4 bytes
        assertLEInt(content, 0, 26);           // totalLen=26
        assertLEInt(content, 4, 12);           // type=12
        assertLEInt(content, 8, 1);            // count=1
        assertEquals(0, content[12]);          // isNull=0
        assertLEInt(content, 14, 4);           // dataLength=4
        assertArrayEquals(data, Arrays.copyOfRange(content, 22, 26));
    }

    @Test
    public void serializeString_NormalCase() throws SQLException {
        String str = "test";
        buffer.serializeString(str, false, 13); // type=13 (NCHAR)

        buffer.stopWrite();
        byte[] content = getCompositeContent(buffer.getBuffer());

        int strlen = ByteBufUtil.utf8Bytes(str); // Get UTF-8 byte length
        assertEquals(22 + strlen, content.length);  // Header 22 bytes + UTF-8 bytes of string
        assertLEInt(content, 0, 22 + strlen);       // totalLen=22+strlen
        assertLEInt(content, 4, 13);                // type=13
        assertLEInt(content, 8, 1);                 // count=1
        assertEquals(0, content[12]);               // isNull=0
        assertLEInt(content, 14, strlen);           // dataLength=strlen
        assertArrayEquals(str.getBytes(StandardCharsets.UTF_8),
                Arrays.copyOfRange(content, 22, 22 + strlen));
    }

    // ====================================== Edge Case Tests ======================================
    @Test
    public void edgeCase_ZeroLengthInput() throws SQLException {
        // Test zero-length byte array
        buffer.writeBytes(new byte[0]);
        buffer.stopWrite();
        assertEquals(0, buffer.getBuffer().readableBytes());

        buffer.release();

        // Test zero-length string
        buffer = new AutoExpandingBuffer(INITIAL_BUFFER_SIZE, MAX_COMPONENTS);
        buffer.writeString("");
        buffer.stopWrite();
        assertEquals(0, buffer.getBuffer().readableBytes());
    }

    @Test
    public void edgeCase_ExactlyFitBuffer() throws SQLException {
        // Test data that exactly fills the buffer
        String str = repeatString("a", INITIAL_BUFFER_SIZE);
        buffer.writeString(str);
        buffer.stopWrite();

        assertEquals(1, buffer.getBuffer().numComponents());
        assertArrayEquals(str.getBytes(StandardCharsets.UTF_8),
                getCompositeContent(buffer.getBuffer()));
    }

    @Test
    public void edgeCase_MaxComponentsReached() throws SQLException {
        // Test writing when max components is reached
        AutoExpandingBuffer autoExpandingBuffer = new AutoExpandingBuffer(4, 2); // 4 bytes per component, max 2 components

        try {
            // First write: 4 bytes → first component
            autoExpandingBuffer.writeBytes(new byte[4]);
            // Second write: 4 bytes → second component
            autoExpandingBuffer.writeBytes(new byte[4]);

            // Third write: 1 byte → should throw exception (exceeds max components)
            assertThrows(SQLException.class, () -> autoExpandingBuffer.writeBytes(new byte[4]));
        }finally {
            autoExpandingBuffer.release();
        }
    }

    // ====================================== Exception Handling Tests ======================================
    @Test
    public void testDoubleRelease() {
        buffer.release();
        assertNull(buffer.getBuffer());

        // Releasing again should have no effect
        buffer.release();
        assertNull(buffer.getBuffer());
    }

    @Test(expected = Exception.class)
    public void testWriteAfterStop() throws SQLException {
        buffer.stopWrite();
        try {
            buffer.writeBytes(new byte[1]); // Writing after stopping should throw exception
        } finally {
            buffer.release();  // Ensure resources are released
        }

    }

// ====================================== Enhanced Helper Methods ======================================
    /** Get little-endian integer (LE) */
    private int getLEInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) |
                ((bytes[offset + 1] & 0xFF) << 8) |
                ((bytes[offset + 2] & 0xFF) << 16) |
                ((bytes[offset + 3] & 0xFF) << 24);
    }

    // ====================================== Helper Methods ======================================
    /** Extract complete byte array from CompositeByteBuf */
    private byte[] getCompositeContent(CompositeByteBuf composite) {
        int totalBytes = composite.readableBytes();
        byte[] content = new byte[totalBytes];
        composite.getBytes(0, content);
        ReferenceCountUtil.safeRelease(composite);  // Avoid memory leaks
        return content;
    }

    /** Verify little-endian integer (LE) */
    private void assertLEInt(byte[] bytes, int offset, int expected) {
        int actual = (bytes[offset] & 0xFF) |
                ((bytes[offset + 1] & 0xFF) << 8) |
                ((bytes[offset + 2] & 0xFF) << 16) |
                ((bytes[offset + 3] & 0xFF) << 24);
        assertEquals(expected, actual);
    }

    private void assertLEShort(byte[] bytes, int offset, short expected) {
        int actual = (bytes[offset] & 0xFF) |
                ((bytes[offset + 1] & 0xFF) << 8);
        assertEquals(expected, actual);
    }

    /** Verify little-endian long (LE) */
    private void assertLELong(byte[] bytes, int offset, long expected) {
        long actual = (bytes[offset] & 0xFFL) |
                ((bytes[offset + 1] & 0xFFL) << 8) |
                ((bytes[offset + 2] & 0xFFL) << 16) |
                ((bytes[offset + 3] & 0xFFL) << 24) |
                ((bytes[offset + 4] & 0xFFL) << 32) |
                ((bytes[offset + 5] & 0xFFL) << 40) |
                ((bytes[offset + 6] & 0xFFL) << 48) |
                ((bytes[offset + 7] & 0xFFL) << 56);
        assertEquals(expected, actual);
    }

    /** Verify little-endian double (LE) */
    private void assertLEDouble(byte[] bytes, int offset, double expected) {
        long bits = (bytes[offset] & 0xFFL) |
                ((bytes[offset + 1] & 0xFFL) << 8) |
                ((bytes[offset + 2] & 0xFFL) << 16) |
                ((bytes[offset + 3] & 0xFFL) << 24) |
                ((bytes[offset + 4] & 0xFFL) << 32) |
                ((bytes[offset + 5] & 0xFFL) << 40) |
                ((bytes[offset + 6] & 0xFFL) << 48) |
                ((bytes[offset + 7] & 0xFFL) << 56);
        assertEquals(expected, Double.longBitsToDouble(bits), 0.0001);
    }

    /** JDK8 compatible string repeat method */
    private String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}