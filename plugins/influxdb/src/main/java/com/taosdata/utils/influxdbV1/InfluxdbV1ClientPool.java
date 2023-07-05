package com.taosdata.utils.influxdbV1;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.influxdb.InfluxDB;

/**
 * Influxdb连接池
 *
 * @author ZYP
 */
public class InfluxdbV1ClientPool extends GenericObjectPool<InfluxDB> {

    public InfluxdbV1ClientPool(PooledObjectFactory<InfluxDB> factory) {
        super(factory);
    }

    public InfluxdbV1ClientPool(PooledObjectFactory<InfluxDB> factory, GenericObjectPoolConfig<InfluxDB> config) {
        super(factory, config);
    }

    public InfluxdbV1ClientPool(PooledObjectFactory<InfluxDB> factory, GenericObjectPoolConfig<InfluxDB> config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
    }
}
