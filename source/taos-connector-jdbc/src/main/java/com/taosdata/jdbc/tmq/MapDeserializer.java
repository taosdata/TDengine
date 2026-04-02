package com.taosdata.jdbc.tmq;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MapDeserializer implements Deserializer<Map<String, Object>> {
    @Override
    public Map<String, Object> deserialize(ResultSet data, String topic, String dbName) throws SQLException {
        Map<String, Object> map = new HashMap<>();

        ResultSetMetaData metaData = data.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            map.put(metaData.getColumnLabel(i), data.getObject(i));
        }

        return map;
    }
}
