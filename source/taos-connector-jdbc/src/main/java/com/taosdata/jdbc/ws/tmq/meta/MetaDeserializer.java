package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.taosdata.jdbc.utils.Utils;

import java.io.IOException;

public class MetaDeserializer extends StdDeserializer<Meta> {

    public MetaDeserializer() {
        this(null);
    }

    protected MetaDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Meta deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, IllegalArgumentException {
        ObjectCodec mapper = p.getCodec();
        JsonNode node = mapper.readTree(p);

        String type = node.get("type").asText();

        String tableType = null;
        if (null != node.get("tableType")){
            tableType = node.get("tableType").asText();
        }

        return getMetaBasedOnTableType(mapper, node, type, tableType);
    }

    private Meta getMetaBasedOnTableType(ObjectCodec mapper, JsonNode node, String type, String tableType) throws JsonProcessingException {
        MetaType metaType = MetaType.fromString(type);
        TableType tblType = TableType.fromString(tableType);

        switch (metaType) {
            case CREATE:
                return createMeta(mapper, node, tblType);
            case DROP:
                return dropMeta(mapper, node, tblType);
            case ALTER:
                return mapper.treeToValue(node, MetaAlterTable.class);
            case DELETE:
                MetaDeleteData meta = mapper.treeToValue(node, MetaDeleteData.class);
                meta.setSql(Utils.unescapeUnicode(meta.getSql()));
                return meta;
            default:
                throw new IllegalArgumentException("Unsupported MetaType: " + metaType);
        }
    }

    private Meta createMeta(ObjectCodec mapper, JsonNode node, TableType tableType) throws JsonProcessingException {
        switch (tableType) {
            case SUPER:
                return mapper.treeToValue(node, MetaCreateSuperTable.class);
            case NORMAL:
                return mapper.treeToValue(node, MetaCreateNormalTable.class);
            case CHILD:
                return mapper.treeToValue(node, MetaCreateChildTable.class);
            default:
                throw new IllegalArgumentException("Unsupported TableType for CREATE: " + tableType);
        }
    }
    private Meta dropMeta(ObjectCodec mapper, JsonNode node, TableType tableType) throws JsonProcessingException {
        if (tableType == TableType.SUPER) {
            return mapper.treeToValue(node, MetaDropSuperTable.class);
        } else {
            return mapper.treeToValue(node, MetaDropTable.class);
        }
    }
}