package com.taosdata.jdbc.tmq;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MapEnhanceDeserializer implements Deserializer<TMQEnhMap> {

    // this method will not be called.
    @Override
    public TMQEnhMap deserialize(ResultSet data, String topic, String dbName) throws SQLException {
        return null;
    }
}
