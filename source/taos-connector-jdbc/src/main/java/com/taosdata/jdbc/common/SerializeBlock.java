package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.DateTimeUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;

public class SerializeBlock {
    private SerializeBlock() {
    }
    private static void serializeShort(ByteBuf buf, short v){
        buf.writeShortLE(v);
    }

    public static void serializeByteArray(ByteBuf buf, byte[] data) {
        buf.writeBytes(data);
    }

    private static void serializeColumn(ColumnInfo columnInfo, ByteBuf buf, int precision) throws SQLException {
        Integer dataLen = DataLengthCfg.getDataLength(columnInfo.getType());

        // TotalLength
        buf.writeIntLE(columnInfo.getSerializeSize());

        // Type
        buf.writeIntLE(columnInfo.getType());
        // Num
        buf.writeIntLE(columnInfo.getDataList().size());
        // IsNull
        for (Object o: columnInfo.getDataList()) {
            if (o == null) {
                buf.writeByte(1);
            } else {
                buf.writeByte(0);
            }
        }

        // haveLength
        if (dataLen != null){
            buf.writeByte(0);
            // buffer
            serializeNormalDataType(columnInfo.getType(), buf, columnInfo.getDataList(), precision);
            return;
        }

        // data is array type
        buf.writeByte(1);
        // length
        int bufferLength = 0;
        for (Object o: columnInfo.getDataList()){
            if (o == null){
                buf.writeIntLE(0);
            } else {
                switch (columnInfo.getType()) {
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_BLOB:
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                    case TSDB_DATA_TYPE_DECIMAL64:
                    case TSDB_DATA_TYPE_DECIMAL128:
                    case TSDB_DATA_TYPE_NCHAR:{
                        byte[] v = (byte[]) o;
                        buf.writeIntLE(v.length);
                        bufferLength += v.length;
                        break;
                    }
                    default:
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + columnInfo.getType());
                }
            }
        }

        // buffer length
        buf.writeIntLE(bufferLength);

        // buffer
        serializeArrayDataType(columnInfo.getType(), buf, columnInfo.getDataList());
        return;
    }

    private static void serializeNormalDataType(int dataType , ByteBuf buf, List<Object> objectList, int precision) throws SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_BOOL: {
                buf.writeIntLE(objectList.size());

                for (Object o: objectList){
                    if (o == null) {
                        buf.writeByte(0);
                    } else {
                        boolean v = (Boolean) o;
                        buf.writeByte(v ? 1 : 0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_TINYINT: {
                buf.writeIntLE(objectList.size());

                for (Object o: objectList){
                    if (o == null) {
                        buf.writeByte(0);
                    } else {
                        byte v = (Byte) o;
                        buf.writeByte(v);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_UTINYINT: {
                buf.writeIntLE(objectList.size());

                for (Object o: objectList){
                    if (o == null) {
                        buf.writeByte(0);
                    } else {
                        short v = (Short) o;
                        if (v < 0 || v > MAX_UNSIGNED_BYTE){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "utinyint value is out of range");
                        }
                        buf.writeByte((v & 0xFF));
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_SMALLINT: {
                buf.writeIntLE(objectList.size() * Short.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        serializeShort(buf, (Short) o);
                    } else {
                        serializeShort(buf, (short)0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_USMALLINT: {
                buf.writeIntLE(objectList.size() * Short.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        int v = (Integer) o;
                        if (v < 0 || v > MAX_UNSIGNED_SHORT){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "usmallint value is out of range");
                        }
                        buf.writeShortLE((v & 0xFFFF));
                    } else {
                        buf.writeShortLE(0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_INT: {
                buf.writeIntLE(objectList.size() * Integer.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        buf.writeIntLE((Integer) o);
                    } else {
                        buf.writeIntLE(0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_UINT: {
                buf.writeIntLE(objectList.size() * Integer.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        long v = (Long) o;
                        if (v < 0 || v > MAX_UNSIGNED_INT){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "uint value is out of range");
                        }
                         buf.writeIntLE((int)(v & 0xFFFFFFFFL));

                    } else {
                        buf.writeIntLE(0);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_BIGINT: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        buf.writeLongLE((Long)o);
                    } else {
                        buf.writeLongLE(0L);
                    }
                }
               break;
            }
            case TSDB_DATA_TYPE_UBIGINT: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        BigInteger v = (BigInteger) o;
                        if (v.compareTo(BigInteger.ZERO) < 0 || v.compareTo(new BigInteger(MAX_UNSIGNED_LONG)) > 0){
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "ubigint value is out of range");
                        }
                        buf.writeLongLE(v.longValue());
                    } else {
                        buf.writeLongLE(0L);
                    }
                }
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                buf.writeIntLE(objectList.size() * Integer.BYTES);

                for (Object o: objectList){
                    float v = 0;
                    if (o != null) {
                        v = (Float) o;
                    }
                    buf.writeFloatLE(v);
                }
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    double v = 0;
                    if (o != null) {
                        v = (Double) o;
                    }
                    buf.writeDoubleLE(v);
                }
                break;
            }
            case TSDB_DATA_TYPE_TIMESTAMP: {
                buf.writeIntLE(objectList.size() * Long.BYTES);

                for (Object o: objectList){
                    if (o != null) {
                        if (o instanceof Instant){
                            Instant instant = (Instant) o;
                            long v = DateTimeUtils.toLong(instant, precision);

                            buf.writeLongLE(v);
                        } else if (o instanceof OffsetDateTime){
                            OffsetDateTime offsetDateTime = (OffsetDateTime) o;
                            long v = DateTimeUtils.toLong(offsetDateTime.toInstant(), precision);

                            buf.writeLongLE(v);
                        } else if (o instanceof ZonedDateTime){
                            ZonedDateTime zonedDateTime = (ZonedDateTime) o;
                            long v = DateTimeUtils.toLong(zonedDateTime.toInstant(), precision);

                            buf.writeLongLE(v);
                        } else if (o instanceof Long){
                            buf.writeLongLE((Long) o);
                        } else {
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported timestamp data type : " + o.getClass().getName());
                        }

                    } else {
                        buf.writeLongLE(0L);
                    }
                }
                break;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + dataType);
        }
    }

    private static void serializeArrayDataType(int dataType, ByteBuf buf, List<Object> objectList) throws SQLException {
        switch (dataType) {
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_DECIMAL64:
            case TSDB_DATA_TYPE_DECIMAL128:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_NCHAR: {
                for (Object o: objectList){
                    if (o != null) {
                        byte[] v = (byte[]) o;
                        serializeByteArray(buf, v);
                    }
                }
                break;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + dataType);
        }
    }
    public static int getTagTotalLength(TableInfo tableInfo, int toBebindTagCount) throws SQLException{
        int totalLength = 0;
        if (toBebindTagCount > 0){
            if (tableInfo.getTagInfo().size() != toBebindTagCount){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table tag size is not match");
            }

            for (ColumnInfo tag : tableInfo.getTagInfo()){
                if (tag.getDataList().isEmpty()){
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "tag value is null, tbname: " + tableInfo.getTableName().toString());
                }
                int columnSize = getColumnSize(tag);
                tag.setSerializeSize(columnSize);
                totalLength += columnSize;
            }
        }
        return totalLength;
    }

    public static int getColTotalLength(TableInfo tableInfo, int toBebindColCount) throws SQLException{
        int totalLength = 0;
        if (toBebindColCount > 0){
            if (tableInfo.getDataList().size() != toBebindColCount){
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table column size is not match");
            }

            for (ColumnInfo columnInfo : tableInfo.getDataList()){
                int columnSize = getColumnSize(columnInfo);
                columnInfo.setSerializeSize(columnSize);
                totalLength += columnSize;
            }
        }
        return totalLength;
    }

    public static int getColumnSize(ColumnInfo column) throws SQLException {
        Integer dataLen = DataLengthCfg.getDataLength(column.getType());

        if (dataLen != null) {
            // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + size * dataLen
            return 17 + (dataLen + 1) * column.getDataList().size();
        }

        switch (column.getType()) {
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_BLOB:
            case TSDB_DATA_TYPE_DECIMAL64:
            case TSDB_DATA_TYPE_DECIMAL128:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_NCHAR:{
                int totalLength = 0;
                for (Object o : column.getDataList()) {
                    if (o != null) {
                        byte[] v = (byte[]) o;
                        totalLength += v.length;
                    }
                }
                // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + 4 * v.length + totalLength
                return 17 + (5 * column.getDataList().size()) + totalLength;
            }
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported data type : " + column.getType());
        }
    }

    public static ByteBuf getStmt2BindBlock(HashMap<ByteBuffer, TableInfo> tableInfoMap,
                                            StmtInfo stmtInfo,
                                            long reqId) throws SQLException {
        return getStmt2BindBlock(reqId,
                stmtInfo.getStmtId(),
                tableInfoMap,
                stmtInfo.getToBeBindTableNameIndex(),
                stmtInfo.getToBeBindTagCount(),
                stmtInfo.getToBeBindColCount(),
                stmtInfo.getPrecision());
    }
    public static ByteBuf getStmt2BindBlock(long reqId,
                                           long stmtId,
                                           HashMap<ByteBuffer, TableInfo> tableInfoMap,
                                           int toBeBindTableNameIndex,
                                           int toBebindTagCount,
                                           int toBebindColCount,
                                           int precision) throws SQLException {

        // cloc totol size
        int totalTableNameSize  = 0;
        List<Short> tableNameSizeList = new ArrayList<>();
        if (toBeBindTableNameIndex >= 0) {
            for (TableInfo tableInfo: tableInfoMap.values()) {
                if (tableInfo.getTableName().capacity() == 0){
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                }
                int tableNameSize = tableInfo.getTableName().capacity() + 1;
                totalTableNameSize += tableNameSize;
                tableNameSizeList.add((short) tableNameSize);
            }
        }

        int totalTagSize = 0;
        List<Integer> tagSizeList = new ArrayList<>();
        if (toBebindTagCount > 0){
            for (TableInfo tableInfo : tableInfoMap.values()) {
                int tagSize = getTagTotalLength(tableInfo, toBebindTagCount);
                totalTagSize += tagSize;
                tagSizeList.add(tagSize);
            }
        }


        int totalColSize = 0;
        List<Integer> colSizeList = new ArrayList<>();
        if (toBebindColCount > 0) {
            for (TableInfo tableInfo : tableInfoMap.values()) {
                int colSize = getColTotalLength(tableInfo, toBebindColCount);
                totalColSize += colSize;
                colSizeList.add(colSize);
            }
        }

        int totalSize = totalTableNameSize + totalTagSize + totalColSize;
        int toBebindTableNameCount = toBeBindTableNameIndex >= 0 ? 1 : 0;

        totalSize += tableInfoMap.size() * (
                toBebindTableNameCount * Short.BYTES
                + (toBebindTagCount > 0 ? 1 : 0) * Integer.BYTES
                + (toBebindColCount > 0 ? 1 : 0) * Integer.BYTES);

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(58 + totalSize);

        try {
            //************ header *****************
            // ReqId
            buf.writeLongLE(reqId);
            // stmtId
            buf.writeLongLE(stmtId);
            // actionId
            buf.writeLongLE(9L);
            // version
            buf.writeShortLE(1);
            // col_idx
            buf.writeIntLE(-1);

            //************ data *****************
            // TotalLength
            buf.writeIntLE(totalSize + 28);
            // tableCount
            buf.writeIntLE(tableInfoMap.size());
            // TagCount
            buf.writeIntLE(toBebindTagCount);
            // ColCount
            buf.writeIntLE(toBebindColCount);
            // tableNameOffset
            if (toBebindTableNameCount > 0) {
                buf.writeIntLE(0x1C);
            } else {
                buf.writeIntLE(0);
            }

            // tagOffset
            if (toBebindTagCount > 0) {
                if (toBebindTableNameCount > 0) {
                    buf.writeIntLE(28 + totalTableNameSize + Short.BYTES * tableInfoMap.size());
                } else {
                    buf.writeIntLE(28);
                }
            } else {
                buf.writeIntLE(0);
            }

            // colOffset
            if (toBebindColCount > 0) {
                int skipSize = 0;
                if (toBebindTableNameCount > 0) {
                    skipSize += totalTableNameSize + Short.BYTES * tableInfoMap.size();
                }

                if (toBebindTagCount > 0) {
                    skipSize += totalTagSize + Integer.BYTES * tableInfoMap.size();
                }
                buf.writeIntLE(28 + skipSize);
            } else {
                buf.writeIntLE(0);
            }

            // TableNameLength
            if (toBebindTableNameCount > 0) {
                for (Short tableNameLen : tableNameSizeList) {
                    if (tableNameLen == 0) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                    }

                    buf.writeShortLE(tableNameLen);
                }

                for (TableInfo tableInfo : tableInfoMap.values()) {
                    if (tableInfo.getTableName().capacity() == 0) {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name is empty");
                    }

                    buf.writeBytes(tableInfo.getTableName().array());
                    buf.writeByte(0);
                }
            }

            // TagsDataLength
            if (toBebindTagCount > 0) {
                for (Integer tagsize : tagSizeList) {
                    buf.writeIntLE(tagsize);
                }

                for (TableInfo tableInfo : tableInfoMap.values()) {
                    for (ColumnInfo tag : tableInfo.getTagInfo()) {
                        if (tag.getDataList().isEmpty()) {
                            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "tag value is null, tbname: " + tableInfo.getTableName().toString());
                        }
                        serializeColumn(tag, buf, precision);
                    }
                }
            }

            // ColsDataLength
            if (toBebindColCount > 0) {
                for (Integer colSize : colSizeList) {
                    buf.writeIntLE(colSize);
                }

                for (TableInfo tableInfo : tableInfoMap.values()) {
                    for (ColumnInfo col : tableInfo.getDataList()) {
                        serializeColumn(col, buf, precision);
                    }
                }
            }


//        for (int i = 30; i < buf.capacity(); i++) { // NOSONAR
//            int bb = buf.getByte(i) & 0xff;
//            System.out.print(bb);
//            System.out.print(",");
//        }
//        System.out.println();
            return buf;
        } catch (Exception e){
            Utils.releaseByteBuf(buf);
            throw e;
        }
    }
}
