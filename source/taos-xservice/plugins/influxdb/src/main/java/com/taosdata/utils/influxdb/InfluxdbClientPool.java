package com.taosdata.utils.influxdb;

import com.influxdb.client.InfluxDBClient;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Influxdb连接池
 *
 * @author ZYP
 */
public class InfluxdbClientPool extends GenericObjectPool<InfluxDBClient> {

    public InfluxdbClientPool(PooledObjectFactory<InfluxDBClient> factory) {
        super(factory);
    }

    public InfluxdbClientPool(PooledObjectFactory<InfluxDBClient> factory, GenericObjectPoolConfig<InfluxDBClient> config) {
        super(factory, config);
    }

    public InfluxdbClientPool(PooledObjectFactory<InfluxDBClient> factory, GenericObjectPoolConfig<InfluxDBClient> config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
    }
}
