package com.taosdata.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public interface TaosPrepareStatement extends PreparedStatement {

    void setTableName(String name) throws SQLException;

    void setTagNull(int index, int type) throws SQLException;

    void setTagBoolean(int index, boolean value);

    void setTagByte(int index, byte value);

    void setTagShort(int index, short value);

    void setTagInt(int index, int value);

    void setTagLong(int index, long value);

    void setTagBigInteger(int index, BigInteger value) throws SQLException;

    void setTagFloat(int index, float value);

    void setTagDouble(int index, double value);

    void setTagTimestamp(int index, long value);

    void setTagTimestamp(int index, Timestamp value);

    void setTagString(int index, String value);

    void setTagVarbinary(int index, byte[] value);
    void setTagGeometry(int index, byte[] value);
    void setTagNString(int index, String value);

    void setTagJson(int index, String value);
    void setInt(int columnIndex, List<Integer> list) throws SQLException;
    void setFloat(int columnIndex, List<Float> list) throws SQLException;

    void setTimestamp(int columnIndex, List<Long> list) throws SQLException;

    void setLong(int columnIndex, List<Long> list) throws SQLException;
    void setBigInteger(int columnIndex, List<BigInteger> list) throws SQLException;
    void setBigDecimal(int columnIndex, List<BigDecimal> list) throws SQLException;

    void setDouble(int columnIndex, List<Double> list) throws SQLException;

    void setBoolean(int columnIndex, List<Boolean> list) throws SQLException;

    void setByte(int columnIndex, List<Byte> list) throws SQLException;

     void setShort(int columnIndex, List<Short> list) throws SQLException;

    void setString(int columnIndex, List<String> list, int size) throws SQLException;

     void setVarbinary(int columnIndex, List<byte[]> list, int size) throws SQLException;
     void setGeometry(int columnIndex, List<byte[]> list, int size) throws SQLException;

    void setBlob(int columnIndex, List<Blob> list, int size) throws SQLException;
    // note: expand the required space for each NChar character
    void setNString(int columnIndex, List<String> list, int size) throws SQLException;

    void columnDataAddBatch() throws SQLException;

    void columnDataExecuteBatch() throws SQLException;
}
