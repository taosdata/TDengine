package com.taosdata.utils.influxdbV1;

import lombok.Setter;
import okhttp3.OkHttpClient;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.concurrent.TimeUnit;

/**
 * 连接池工厂
 *
 * @author ZYP
 */
public class InfluxdbV1PooledObjectFactory implements PooledObjectFactory<InfluxDB> {

    @Setter
    private String url;
    @Setter
    private String username;
    @Setter
    private String password;

    /**
     * 重新初始化要由池返回的实例-即从池中借用一个对象时调用
     *
     * @param pooledObject 一个PooledObject包装要激活的实例
     * @throws Exception
     */
    @Override
    public void activateObject(PooledObject<InfluxDB> pooledObject) {
        // 重新初始化要由池返回的实例-即从池中借用一个对象时调用
    }

    /**
     * 使用默认(NORMAL)DestroyMode销毁池不再需要的实例
     *
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void destroyObject(PooledObject<InfluxDB> pooledObject) {
        InfluxDB influxDB = pooledObject.getObject();
        influxDB.close();
    }

    /**
     * 创建可由池提供服务的实例，并将其包装在由池管理的PooledObject中
     *
     * @return
     * @throws Exception
     */
    @Override
    public PooledObject<InfluxDB> makeObject() {
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder()
            .connectTimeout(60, TimeUnit.SECONDS)  // 建立连接超时
            .readTimeout(3600, TimeUnit.SECONDS)     // 读取数据超时
            .writeTimeout(3600, TimeUnit.SECONDS);    // 写入数据超时
        // 生成客户端
        InfluxDB influxDB = InfluxDBFactory.connect(this.url, this.username, this.password, okHttpClientBuilder).enableGzip();
        return new DefaultPooledObject<>(influxDB);
    }

    /**
     * 取消初始化要返回到空闲对象池的实例-即从池中归还一个对象时调用
     *
     * @param pooledObject
     * @throws Exception
     */
    @Override
    public void passivateObject(PooledObject<InfluxDB> pooledObject) {
        // 取消初始化要返回到空闲对象池的实例-即从池中归还一个对象时调用
    }

    /**
     * 确保实例可以安全地由池返回。
     *
     * @param pooledObject
     * @return 如果obj无效并且应该从池中删除，则为false ，否则为true
     */
    @Override
    public boolean validateObject(PooledObject<InfluxDB> pooledObject) {
        InfluxDB influxDB = pooledObject.getObject();
        return influxDB.ping().isGood();
    }
}
