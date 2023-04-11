package com.taosdata.utils.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.taosdata.config.InfluxdbConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

/**
 * 连接池自动配置
 *
 * @author ZYP
 */
@Configuration
public class InfluxdbPoolAutoConfig {

    @Resource
    private InfluxdbConfig influxdbConfig;

    private InfluxdbClientPool pool;

    @ConditionalOnClass({InfluxdbPooledObjectFactory.class})
    @Bean("influxdbClientPool")
    protected InfluxdbClientPool createInluxdbClientPool() {
        InfluxdbPooledObjectFactory factory = new InfluxdbPooledObjectFactory();
        // 设置连接参数
        factory.setUrl(influxdbConfig.getUrl());
        factory.setToken(influxdbConfig.getToken());
        // 设置对象池相关参数
        GenericObjectPoolConfig<InfluxDBClient> poolConfig = new GenericObjectPoolConfig<>();
        // 最大空闲
        poolConfig.setMaxIdle(influxdbConfig.getMaxIdle());
        // 最大总数
        poolConfig.setMaxTotal(influxdbConfig.getMaxTotal());
        // 最小空闲
        poolConfig.setMinIdle(influxdbConfig.getMinIdle());
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(1000 * 60 * 30);
        //一定要关闭jmx，不然springboot启动会报已经注册了某个jmx的错误
        poolConfig.setJmxEnabled(false);
        // 新建一个对象池，传入对象工厂和配置
        pool = new InfluxdbClientPool(factory, poolConfig);
        // 初始化连接池
        initPool(influxdbConfig.getInitialSize(), influxdbConfig.getMaxIdle());
        return pool;
    }

    /**
     * 预先加载testObject对象到对象池中
     *
     * @param initialSize 初始化连接数
     * @param maxIdle     最大空闲连接数
     */
    private void initPool(int initialSize, int maxIdle) {
        if (initialSize <= 0) {
            return;
        }
        int size = Math.min(initialSize, maxIdle);
        for (int i = 0; i < size; i++) {
            try {
                pool.addObject();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        if (pool != null) {
            pool.close();
        }
    }
}
