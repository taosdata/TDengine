package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.TDBlob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BlobUtil {
    private BlobUtil() {
        // Prevent instantiation
    }

    public static byte[] getBytes(java.sql.Blob blob) throws SQLException {
        return blob.getBytes(1, (int) blob.length());
    }

    public static byte[] getFromInputStream(InputStream inputStream, long length) throws SQLException {
        if (inputStream == null || length < 0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "InputStream/length invalid");
        }

        try {
            byte[] bytes = new byte[(int) length];
            int readLen = inputStream.read(bytes);
            if (readLen != length) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "InputStream length mismatch: expected " + length + ", got " + readLen);
            }
            return bytes;
        } catch (IOException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Failed to read InputStream: " + e.getMessage());
        }
    }

    public static byte[] getFromInputStream(InputStream inputStream) throws SQLException {
        if (inputStream == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "InputStream is null");
        }

        Blob blob = new TDBlob(new byte[0], false);

        try (InputStream in = inputStream;
             OutputStream out = blob.setBinaryStream(1)) {

            byte[] buffer = new byte[100 * 1024];
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "io error: " + e.getMessage());
        }
        return blob.getBytes(1, (int) blob.length());
    }

    public static List<byte[]> getListBytes(List<Blob> blobList) throws SQLException {
        List<byte[]> collect = new ArrayList<>();
        for (Blob blob : blobList) {
            if (blob == null) {
                collect.add(null);
            } else {
                try {
                    collect.add(blob.getBytes(1, (int) blob.length()));
                } catch (SQLException e) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Error processing Blob getBytes: " + e.getMessage());
                }
            }
        }
        return collect;
    }
}
