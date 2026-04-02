package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.enums.DataType;
import com.taosdata.jdbc.enums.TmqMessageType;
import com.taosdata.jdbc.rs.RestfulResultSet;
import com.taosdata.jdbc.rs.RestfulResultSetMetaData;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQEnhMap;
import com.taosdata.jdbc.tmq.TopicPartition;
import com.taosdata.jdbc.utils.*;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.tmq.ConsumerAction;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.taosdata.jdbc.TSDBConstants.*;

public class FetchRawBlockResp extends Response {
    private ByteBuf buffer;
    private long time;
    private int code;
    private String message;
    private short version;

    private RestfulResultSetMetaData metaData;
    private final List<RestfulResultSet.Field> fields = new ArrayList<>();
    private final List<String> columnNames = new ArrayList<>();
    // data
    private List<List<Object>> resultData;
    private byte precision;
    private int rows = 0;
    private String tableName = "";

    public FetchRawBlockResp(ByteBuf buffer){
        this.setAction(ConsumerAction.FETCH_RAW_DATA.getAction());
        this.buffer = buffer;
    }

    public void init() {
        buffer.readLongLE(); // action id
        version = buffer.readShortLE();
        time = buffer.readLongLE();
        this.setReqId(buffer.readLongLE());
        code = buffer.readIntLE();
        int messageLen = buffer.readIntLE();
        byte[] msgBytes = new byte[messageLen];
        buffer.readBytes(msgBytes);

        message = new String(msgBytes, StandardCharsets.UTF_8);
        buffer.readLongLE(); // message id
        buffer.readShortLE(); // message type
        buffer.readIntLE(); // raw block length
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuf buffer) {
        this.buffer = buffer;
    }
    public long getTime() {
        return time;
    }
    public int getCode() {
        return code;
    }
    public String getMessage() {
        return message;
    }
    public short getVersion() {
        return version;
    }


    private void skipHead() throws SQLException {
        byte version = buffer.readByte();
        if (version >= 100) {
            int skip = buffer.readIntLE();
            buffer.skipBytes(skip);
        } else {
            int skip = getTypeSkip(version);
            buffer.skipBytes(skip);

            version = buffer.readByte();
            skip = getTypeSkip(version);
            buffer.skipBytes(skip);
        }
    }

    private int getTypeSkip(byte type) throws SQLException {
        switch (type) {
            case 1:
                return 8;
            case 2:
            case 3:
                return 16;
            default:
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "FetchBlockRawResp getTypeSkip error, type: " + type);
        }
    }

    public void parseBlockInfos() throws SQLException {
        skipHead();
        int blockNum = buffer.readIntLE();
        int cols = 0;

        boolean withTableName = buffer.readByte() != 0;// skip withTableName
        buffer.readByte();// skip withSchema

        for (int i = 0; i < blockNum; i++) {
            int blockTotalLen = parseVariableByteInteger();
            buffer.skipBytes(17);
            precision = buffer.readByte();

            // only parse the first block's schema
            if (i == 0){
                buffer.markReaderIndex();
                buffer.skipBytes(blockTotalLen - 18);
                cols = parseZigzagVariableByteInteger();
                resultData = new ArrayList<>(cols);
                for (int j = 0; j < cols; j++) {
                    resultData.add(new ArrayList<>());
                }
                //version
                parseZigzagVariableByteInteger();
                for (int j = 0; j < cols; j++) {
                    RestfulResultSet.Field field = parseSchema();
                    fields.add(field);
                    columnNames.add(field.getName());
                }
                if(withTableName){
                    tableName = parseName();
                }
                buffer.resetReaderIndex();
            }
            fetchBlockData();
            buffer.readByte(); // skip useless byte

            parseZigzagVariableByteInteger(); // skip ncols
            parseZigzagVariableByteInteger(); // skip version
            for (int j = 0; j < cols; j++) {
                skipSchema(withTableName);
            }
            if (withTableName){
                int tableNameLen = parseVariableByteInteger();
                buffer.skipBytes(tableNameLen);
            }
        }
        if (!resultData.isEmpty()){
            // rows is the number of rows of the first column
            rows = resultData.get(0).size();
        }
        buffer.release();
    }
    public ConsumerRecords<TMQEnhMap> getEhnMapListInner(PollResp pollResp, ZoneId zoneId, boolean varcharAsString) throws SQLException {
        skipHead();
        int blockNum = buffer.readIntLE();
        int cols = 0;

        ConsumerRecords<TMQEnhMap> records = new ConsumerRecords<>();
        boolean withTableName = buffer.readByte() != 0;// skip withTableName
        buffer.readByte();// skip withSchema

        for (int i = 0; i < blockNum; i++) {
            int blockTotalLen = parseVariableByteInteger();
            buffer.skipBytes(17);
            precision = buffer.readByte();

            fields.clear();
            columnNames.clear();

            // parse the block's schema
            buffer.markReaderIndex();
            buffer.skipBytes(blockTotalLen - 18);
            cols = parseZigzagVariableByteInteger();
            resultData = new ArrayList<>(cols);
            for (int j = 0; j < cols; j++) {
                resultData.add(new ArrayList<>());
            }
            //version
            parseZigzagVariableByteInteger();
            for (int j = 0; j < cols; j++) {
                RestfulResultSet.Field field = parseSchema();
                fields.add(field);
                columnNames.add(field.getName());
            }
            if(withTableName){
                tableName = parseName();
            }
            buffer.resetReaderIndex();

            fetchBlockData();
            buffer.readByte(); // skip useless byte

            parseZigzagVariableByteInteger(); // skip ncols
            parseZigzagVariableByteInteger(); // skip version
            for (int j = 0; j < cols; j++) {
                skipSchema(withTableName);
            }

            if (withTableName){
                int tableNameLen = parseVariableByteInteger();
                buffer.skipBytes(tableNameLen);
            }

            // handle the data in this block
            int lineNum = resultData.get(0).size();
            for (int j = 0; j < lineNum; j++) {
                HashMap<String, Object> lineDataMap = new HashMap<>();
                for (int k = 0; k < cols; k++) {
                    if (fields.get(k).getTaosType() == TSDB_DATA_TYPE_TIMESTAMP){
                        Long o = (Long) DataTypeConvertUtil.parseValue(TSDB_DATA_TYPE_TIMESTAMP, resultData.get(k).get(j), varcharAsString);
                        Instant instant = DateTimeUtils.parseTimestampColumnData(o, precision);
                        Timestamp t = DateTimeUtils.getTimestamp(instant, zoneId);
                        lineDataMap.put(columnNames.get(k), t);
                        continue;
                    }
                    Object o = DataTypeConvertUtil.parseValue(fields.get(k).getTaosType(), resultData.get(k).get(j), varcharAsString);
                    lineDataMap.put(columnNames.get(k), o);
                }
                TMQEnhMap map = new TMQEnhMap(tableName, lineDataMap);
                ConsumerRecord<TMQEnhMap> r = new ConsumerRecord.Builder<TMQEnhMap>()
                        .topic(pollResp.getTopic())
                        .dbName(pollResp.getDatabase())
                        .vGroupId(pollResp.getVgroupId())
                        .offset(pollResp.getOffset())
                        .messageType(TmqMessageType.TMQ_RES_DATA)
                        .meta(null)
                        .value(map)
                        .build();
                TopicPartition tp = new TopicPartition(pollResp.getTopic(), pollResp.getVgroupId());
                records.put(tp, r);
            }
            resultData.clear();
        }

        return records;
    }
    public ConsumerRecords<TMQEnhMap> getEhnMapList(PollResp pollResp, ZoneId zoneId, boolean varcharAsString) throws SQLException {
        try {
            return getEhnMapListInner(pollResp, zoneId, varcharAsString);
        } finally {
            Utils.releaseByteBuf(buffer);
        }
    }


        private int parseVariableByteInteger() {
        int multiplier = 1;
        int value = 0;
        while (true) {
            int encodedByte = buffer.readByte();
            value += (encodedByte & 127) * multiplier;
            if ((encodedByte & 128) == 0) {
                break;
            }
            multiplier *= 128;
        }
        return value;
    }

    private int zigzagDecode(int n) {
        return (n >> 1) ^ (-(n & 1));
    }

    private int parseZigzagVariableByteInteger() {
        return zigzagDecode(parseVariableByteInteger());
    }
    private String parseName() {
        int nameLen = parseVariableByteInteger();
        byte[] name = new byte[nameLen - 1];
        buffer.readBytes(name);
        buffer.skipBytes(1);
        return new String(name, StandardCharsets.UTF_8);
    }

    private RestfulResultSet.Field parseSchema() throws SQLException{
        int taosType = buffer.readByte();
        int jdbcType = DataType.convertTaosType2DataType(taosType).getJdbcTypeValue();
        buffer.readByte(); // skip flag
        int bytes = parseZigzagVariableByteInteger();
        parseZigzagVariableByteInteger(); // skip colid
        String name = parseName();
        return new RestfulResultSet.Field(name, jdbcType, bytes, "", taosType, 0, 0);
    }

    private void skipSchema(boolean withTableName){
        buffer.skipBytes(2);
        parseZigzagVariableByteInteger(); // skip bytes
        parseZigzagVariableByteInteger(); // skip colld
        int nameLen = parseVariableByteInteger();
        buffer.skipBytes(nameLen);
    }

    private int getScaleFromRowBlock(ByteBuf buffer, int pHeader, int colIndex) {
        // for decimal: |___bytes___|__empty__|___prec___|__scale___|
        int backupPos = buffer.readerIndex();
        buffer.readerIndex(pHeader);
        buffer.skipBytes(colIndex * 5 + 1);
        int scale = buffer.readIntLE();
        buffer.readerIndex(backupPos);
        return scale & 0xFF;
    }

    private void fetchBlockData() throws SQLException {
        buffer.skipBytes(8);
        int numOfRows = buffer.readIntLE();
        int bitMapOffset = bitmapLen(numOfRows);
        int beforeColLenPos = buffer.readerIndex() + 16;

        int pHeader = buffer.readerIndex() + 16 + fields.size() * 5;
        buffer.readerIndex(pHeader);
        List<Integer> lengths = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            lengths.add(buffer.readIntLE());
        }
        pHeader = buffer.readerIndex();
        int length = 0;
        for (int i = 0; i < fields.size(); i++) {
            List<Object> col = resultData.get(i);
            int type = fields.get(i).getTaosType();
            int scale = 0;
            if (type == TSDB_DATA_TYPE_DECIMAL128 || type == TSDB_DATA_TYPE_DECIMAL64) {
                scale = getScaleFromRowBlock(buffer, beforeColLenPos, i);
            }
            switch (type) {
                case TSDB_DATA_TYPE_BOOL:
                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.readBytes(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        byte b = buffer.readByte();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(b);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.readBytes(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        short s = buffer.readShortLE();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(s);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.readBytes(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        int in = buffer.readIntLE();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(in);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.readBytes(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        long l = buffer.readLongLE();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(l);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.readBytes(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        float f = buffer.readFloatLE();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(f);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.readBytes(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        double d = buffer.readDoubleLE();
                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            col.add(d);
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_JSON:
                case TSDB_DATA_TYPE_BLOB:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_GEOMETRY:{
                    length = numOfRows * 4;
                    List<Integer> offset = new ArrayList<>(numOfRows);
                    for (int m = 0; m < numOfRows; m++) {
                        offset.add(buffer.readIntLE());
                    }
                    int start = buffer.readerIndex();
                    for (int m = 0; m < numOfRows; m++) {
                        if (-1 == offset.get(m)) {
                            col.add(null);
                            continue;
                        }
                        buffer.readerIndex(start + offset.get(m));
                        int len;
                        if (type != TSDB_DATA_TYPE_BLOB){
                            len = buffer.readShortLE() & 0xFFFF;
                        } else {
                            len = buffer.readIntLE();
                        }
                        byte[] tmp = new byte[len];
                        buffer.readBytes(tmp);
                        if (type != TSDB_DATA_TYPE_BLOB) {
                            col.add(tmp);
                        } else {
                            col.add(new TDBlob(tmp, true));
                        }
                    }
                    break;
                }
                case TSDB_DATA_TYPE_NCHAR: {
                    length = numOfRows * 4;
                    List<Integer> offset = new ArrayList<>(numOfRows);
                    for (int m = 0; m < numOfRows; m++) {
                        offset.add(buffer.readIntLE());
                    }
                    int start = buffer.readerIndex();
                    for (int m = 0; m < numOfRows; m++) {
                        if (-1 == offset.get(m)) {
                            col.add(null);
                            continue;
                        }
                        buffer.readerIndex(start + offset.get(m));
                        int len = (buffer.readShortLE() & 0xFFFF) / 4;
                        int[] tmp = new int[len];
                        for (int n = 0; n < len; n++) {
                            tmp[n] = buffer.readIntLE();
                        }
                        col.add(tmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DECIMAL128:
                case TSDB_DATA_TYPE_DECIMAL64:
                    int dataLen = type == TSDB_DATA_TYPE_DECIMAL128 ? 16 : 8;
                    length = bitMapOffset;
                    byte[] tmp = new byte[bitMapOffset];
                    buffer.readBytes(tmp);
                    for (int j = 0; j < numOfRows; j++) {
                        byte[] tb = new byte[dataLen];
                        buffer.readBytes(tb);

                        if (BlockUtil.isNull(tmp, j)) {
                            col.add(null);
                        } else {
                            BigDecimal t = DecimalUtil.getBigDecimal(tb, scale);
                            col.add(t);
                        }
                    }
                    break;
                default:
                    // unknown type, do nothing
                    col.add(null);
                    break;
            }
            pHeader += length + lengths.get(i);
            buffer.readerIndex(pHeader);
        }
    }

    private int bitmapLen(int n) {
        return (n + 0x7) >> 3;
    }

    public RestfulResultSetMetaData getMetaData() {
        return metaData;
    }

    public List<RestfulResultSet.Field> getFields() {
        return fields;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<Object>> getResultData() {
        return resultData;
    }

    public byte getPrecision() {
        return precision;
    }

    public int getRows() {
        return rows;
    }
    public String getTableName() {
        return tableName;
    }
}
