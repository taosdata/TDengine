package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * TDengine Blob (TDBlob) implementation that wraps in-memory binary data
 * and provides streaming access similar to JDBC Blob interface.
 * This class is designed for scenarios where native database streaming is not available.
 */
public class TDBlob implements java.sql.Blob {
    private byte[] data;       // Underlying binary data storage
    private boolean freed = false; // Flag to indicate if resources have been released
    private boolean readOnly = false; // Indicates this Blob is read-only

    public TDBlob(byte[] data, boolean isReadOnly) {
        this.data = data;
        this.readOnly = isReadOnly;
    }

    @Override
    public long length() throws SQLException {
        checkIfFreed();
        return data.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        checkIfFreed();

        if (pos == 1 && length >= data.length) {
            // Special case: if pos is 1 and length covers entire data, return whole array
            return data;
        }

        // Validate position (JDBC specification: pos starts at 1)
        if (pos < 1 || pos > data.length) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Position out of range");
        }

        int startIdx = (int) (pos - 1); // Convert to 0-based array index
        int endIdx = Math.min(startIdx + length, data.length);

        // Copy specified range of bytes
        byte[] result = new byte[endIdx - startIdx];
        System.arraycopy(data, startIdx, result, 0, result.length);
        return result;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        checkIfFreed();
        return new ByteArrayInputStream(data);
    }

    // Optional methods (implement based on specific requirements)
    @Override
    public long position(Blob pattern, long start) throws SQLException {
        checkIfFreed();
        return position(pattern.getBytes(1, (int) pattern.length()), start);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        checkIfFreed();

        // Validate start position (JDBC position starts at 1)
        if (start < 1 || start > data.length) {
            return -1;
        }

        int fromIndex = (int) (start - 1);
        if (pattern.length == 0) {
            return fromIndex + 1L; // Empty pattern matches immediately
        }

        // Boyer-Moore algorithm: precompute bad character shift table
        int[] badCharShift = new int[256];
        for (int i = 0; i < 256; i++) {
            badCharShift[i] = -1;
        }
        for (int i = 0; i < pattern.length; i++) {
            badCharShift[pattern[i]] = i;
        }

        int i = fromIndex;
        while (i <= data.length - pattern.length) {
            // Compare from end of pattern backwards
            int j = pattern.length - 1;
            while (j >= 0 && data[i + j] == pattern[j]) {
                j--;
            }

            if (j < 0) {
                // Match found
                return i + 1L; // Convert to JDBC 1-based position
            } else {
                // Shift by bad character rule
                int shift = badCharShift[data[i + j] & 0xFF];
                i += Math.max(1, shift);
            }
        }

        return -1; // No match found
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        if (readOnly) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Read-only Blob implementation");
        }

        if (pos != 1) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only setting bytes from the beginning is supported");
        }

        this.data = new byte[bytes.length];
        System.arraycopy(bytes, 0, this.data, 0, bytes.length);
        return bytes.length; // Return number of bytes set
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        if (readOnly) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Read-only Blob implementation");
        }

        // only support setting bytes from the beginning
        if (pos != 1) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only setting bytes from the beginning is supported");
        }

        if (offset + len > bytes.length) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Invalid len, bigger than bytes.length");
        }

        this.data = new byte[len];
        System.arraycopy(bytes, offset, this.data, 0, len);
        return len; // Return number of bytes set
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "Truncate operation is not supported on this Blob");
    }

    @Override
    public void free() throws SQLException {
        // Mark resources as freed to assist GC
        if (!freed){
            freed = true;
            data = null; // Allow garbage collection of the byte array
        }
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        checkIfFreed();

        // Validate position
        if (pos < 1 || pos > data.length) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Position out of range");
        }

        int startIdx = (int) (pos - 1);
        int endIdx = (int) Math.min(startIdx + length, data.length);

        // Create stream for subarray (avoids copying entire array)
        return new ByteArrayInputStream(data, startIdx, endIdx - startIdx);
    }

    // Check if resources have been freed
    private void checkIfFreed() throws SQLException {
        if (freed) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESOURCE_FREEED, "Blob resources have been freed");
        }
    }


    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        checkIfFreed();
        if (readOnly) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Read-only Blob implementation");
        }

        if (pos != 1) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only setting binary stream from the beginning is supported");
        }

        return new TDOutputStream(512 * 1024); // default size of 512KB
    }

    private class TDOutputStream extends ByteArrayOutputStream {
        public TDOutputStream(int size) {
            super(size);
        }

        @Override
        public void close() throws IOException {
            super.close();
            data = super.toByteArray();
        }
    }
}