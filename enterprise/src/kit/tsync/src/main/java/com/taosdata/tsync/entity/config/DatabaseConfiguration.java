package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.DatabasePrecision;

public class DatabaseConfiguration extends Configuration {
    public static final DatabasePrecision DEFAULT_PRECISION = DatabasePrecision.MS;

    private String name;
    private DatabasePrecision precision;

    public DatabaseConfiguration() {
        super(ConfigurationType.DATABASE);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DatabasePrecision getPrecision() {
        return precision;
    }

    public void setPrecision(DatabasePrecision precision) {
        this.precision = precision;
    }
}
