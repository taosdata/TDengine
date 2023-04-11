package com.taosdata.utils.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.HealthCheck;
import lombok.Setter;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * 连接池工厂
 *
 * @author ZYP
 */
public class InfluxdbPooledObjectFactory implements PooledObjectFactory<InfluxDBClient> {

    @Setter
    private String url;
    @Setter
    private String token;

    /**
     * 重新初始化要由池返回的实例-即从池中借用一个对象时调用
     *
     * @param pooledObject 一个PooledObject包装要激活的实例
     * @throws Exception
     */
    @Override
    public void activateObject(PooledObject<InfluxDBClient> pooledObject) throws Exception {
        // 重新初始化要由池返回的实例-即从池中借用一个对象时调用
    }

    /**
     * 使用默认(NORMAL)DestroyMode销毁池不再需要的实例
     *
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void destroyObject(PooledObject<InfluxDBClient> pooledObject) throws Exception {
        InfluxDBClient influxDBClient = pooledObject.getObject();
        influxDBClient.close();
    }

    /**
     * 创建可由池提供服务的实例，并将其包装在由池管理的PooledObject中
     *
     * @return
     * @throws Exception
     */
    @Override
    public PooledObject<InfluxDBClient> makeObject() throws Exception {
        // 生成客户端
        InfluxDBClient client = InfluxDBClientFactory.create(this.url, this.token.toCharArray());
        return new DefaultPooledObject<>(client);
    }

    /**
     * 取消初始化要返回到空闲对象池的实例-即从池中归还一个对象时调用
     *
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void passivateObject(PooledObject<InfluxDBClient> pooledObject) throws Exception {
        // 取消初始化要返回到空闲对象池的实例-即从池中归还一个对象时调用
    }

    /**
     * 确保实例可以安全地由池返回。
     *
     * @param pooledObject
     * @return 如果obj无效并且应该从池中删除，则为false ，否则为true
     */
    @Override
    public boolean validateObject(PooledObject<InfluxDBClient> pooledObject) {
        InfluxDBClient influxDBClient = pooledObject.getObject();
        HealthCheck health = influxDBClient.health();
        return HealthCheck.StatusEnum.PASS.equals(health.getStatus());
    }
}
