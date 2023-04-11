package com.taosdata.caches;

import com.taosdata.model.entity.InfluxdbBucketDataEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 数据缓存
 *
 * @author ZYP
 */
public class BucketDataCache {

    private static Queue<InfluxdbBucketDataEntity> bucketDataQueue = new ConcurrentLinkedQueue<>();

    /**
     * 添加数据并获取队列大小
     *
     * @param influxdbBucketDataEntity
     * @return
     */
    public static int addBucketData(InfluxdbBucketDataEntity influxdbBucketDataEntity) {
        // 放入队列中
        bucketDataQueue.add(influxdbBucketDataEntity);
        // 返回当前队列大小
        return bucketDataQueue.size();
    }

    /**
     * 批量添加数据并获取队列大小
     *
     * @param influxdbBucketDataEntityList
     * @return
     */
    public static int addBucketData(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList) {
        // 放入队列中
        bucketDataQueue.addAll(influxdbBucketDataEntityList);
        // 返回当前队列大小
        return bucketDataQueue.size();
    }

    /**
     * 获取指定数量的数据
     *
     * @param batch
     * @return
     */
    public static List<InfluxdbBucketDataEntity> getBucketData(long batch) {
        List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
        // 遍历获取
        for (int i = 0; i < batch; i++) {
            InfluxdbBucketDataEntity influxdbBucketDataEntity = bucketDataQueue.poll();
            // 非空则放入列表，空则中断
            if (influxdbBucketDataEntity != null) {
                influxdbBucketDataEntityList.add(influxdbBucketDataEntity);
            } else {
                break;
            }
        }
        return influxdbBucketDataEntityList;
    }

    /**
     * 获取队列大小
     *
     * @return
     */
    public static int getBucketDataQueueSize() {
        return bucketDataQueue.size();
    }
}
