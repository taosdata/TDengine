package com.taosdata.utils.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.taosdata.config.InfluxdbConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

/**
 * 连接池自动配置
 *
 * @author ZYP
 */
@Component
public class InfluxdbPoolAutoConfig {

    @Resource
    private InfluxdbConfig influxdbConfig;

    private InfluxdbClientPool pool;

    /**
     * 获取连接池
     *
     * @return
     */
    public InfluxdbClientPool getPool() {
        // 判断连接池状态
        if (this.pool == null || this.pool.isClosed()) {
            createInluxdbClientPool();
        }
        return this.pool;
    }

    /**
     * 创建连接池
     */
    public void createInluxdbClientPool() {
        InfluxdbPooledObjectFactory factory = new InfluxdbPooledObjectFactory();
        // 设置连接参数
        factory.setUrl(this.influxdbConfig.getUrl());
        factory.setToken(this.influxdbConfig.getToken());
        // 设置对象池相关参数
        GenericObjectPoolConfig<InfluxDBClient> poolConfig = new GenericObjectPoolConfig<>();
        // 最大空闲
        poolConfig.setMaxIdle(this.influxdbConfig.getMaxIdle());
        // 最大总数
        poolConfig.setMaxTotal(this.influxdbConfig.getMaxTotal());
        // 最小空闲
        poolConfig.setMinIdle(this.influxdbConfig.getMinIdle());
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(1000 * 60 * 30);
        //一定要关闭jmx，不然springboot启动会报已经注册了某个jmx的错误
        poolConfig.setJmxEnabled(false);
        // 新建一个对象池，传入对象工厂和配置
        this.pool = new InfluxdbClientPool(factory, poolConfig);
        // 初始化连接池
        initPool(this.influxdbConfig.getInitialSize(), this.influxdbConfig.getMaxIdle());
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
                this.pool.addObject();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        if (this.pool != null) {
            this.pool.close();
        }
    }
}
