package com.taosdata.caches;

import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

@ExtendWith(SpringExtension.class)
class BucketDataCacheTest {

    @MockBean
    InfluxdbBucketDataEntity influxdbBucketDataEntity;

    @BeforeEach
    void clearBucketDataCache() {
        BucketDataCache.getBucketData(BucketDataCache.getBucketDataQueueSize());
        BucketDataCache.getBucketDataKeySet().forEach(key -> BucketDataCache.removeBucketDataKey(key));
    }

    @Test
    void addBucketData_List() {
        // 添加元素，队列长度 2
//        Assertions.assertEquals(2, BucketDataCache.addBucketData(Arrays.asList(this.influxdbBucketDataEntity, this.influxdbBucketDataEntity)));
    }

    @Test
    void addBucketData_ByKey() {
        // 添加元素，队列长度 1
//        Assertions.assertEquals(1, BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity));
        // 添加元素，队列长度 2
//        Assertions.assertEquals(2, BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity));
    }

    @Test
    void getBucketData() {
        // 取到队列，空队列
        Assertions.assertEquals(new ArrayList<>(), BucketDataCache.getBucketData(10));
        // 添加元素
        BucketDataCache.addBucketData(Arrays.asList(this.influxdbBucketDataEntity, this.influxdbBucketDataEntity));
        // 取到队列，非空队列
        Assertions.assertEquals(Arrays.asList(this.influxdbBucketDataEntity, this.influxdbBucketDataEntity), BucketDataCache.getBucketData(10));
    }

    @Test
    void getBucketData_ByKey() {
        // 取到队列，空队列
        Assertions.assertEquals(new ArrayList<>(), BucketDataCache.getBucketData("bucket1,measurement1", 10));
        // 添加元素
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        // 取到队列，队列长度为2
        Assertions.assertEquals(Arrays.asList(this.influxdbBucketDataEntity, this.influxdbBucketDataEntity), BucketDataCache.getBucketData("bucket1,measurement1", 10));
    }

    @Test
    void getBucketDataQueueSize() {
        // 获取队列长度 0
        Assertions.assertEquals(0, BucketDataCache.getBucketDataQueueSize());
        // 添加元素
        BucketDataCache.addBucketData(Arrays.asList(this.influxdbBucketDataEntity));
        // 获取队列长度 1
        Assertions.assertEquals(1, BucketDataCache.getBucketDataQueueSize());
        // 添加元素
        BucketDataCache.addBucketData(Arrays.asList(this.influxdbBucketDataEntity));
        // 获取队列长度 2
        Assertions.assertEquals(2, BucketDataCache.getBucketDataQueueSize());
    }

    @Test
    void getBucketDataQueueSize_ByKey() {
        // 获取队列长度 0
        Assertions.assertEquals(0, BucketDataCache.getBucketDataQueueSize("bucket1,measurement1"));
        // 添加元素
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        // 获取队列长度 1
        Assertions.assertEquals(1, BucketDataCache.getBucketDataQueueSize("bucket1,measurement1"));
        // 添加元素
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        // 获取队列长度 2
        Assertions.assertEquals(2, BucketDataCache.getBucketDataQueueSize("bucket1,measurement1"));
    }

    @Test
    void getBucketDataQueueTotalSize() {
        // 获取队列长度 0
        Assertions.assertEquals(0, BucketDataCache.getBucketDataQueueTotalSize());
        // 添加元素
        BucketDataCache.addBucketData(Arrays.asList(this.influxdbBucketDataEntity));
        // 获取队列长度 1
        Assertions.assertEquals(1, BucketDataCache.getBucketDataQueueTotalSize());
        // 添加元素
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        // 获取队列长度 2
        Assertions.assertEquals(2, BucketDataCache.getBucketDataQueueTotalSize());
    }

    @Test
    void getBucketDataQueueTotalSize_ByKey() {
        // 添加元素
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        BucketDataCache.addBucketData("bucket1,measurement2", this.influxdbBucketDataEntity);
        // 获取队列长度 2
        Assertions.assertEquals(2, BucketDataCache.getBucketDataQueueTotalSize("bucket1,"));
        // 获取队列长度 1
        Assertions.assertEquals(1, BucketDataCache.getBucketDataQueueTotalSize("bucket1,measurement1"));
        // 获取队列长度 0
        Assertions.assertEquals(0, BucketDataCache.getBucketDataQueueTotalSize("bucket1,measurement3"));
    }

    @Test
    void getBucketDataKeySet() {
        // 获取集合，空集合
        Assertions.assertEquals(new HashSet<>(), BucketDataCache.getBucketDataKeySet());
        // 添加元素
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        BucketDataCache.addBucketData("bucket1,measurement2", this.influxdbBucketDataEntity);
        // 获取集合，非空集合
        Assertions.assertEquals(new HashSet<>(Arrays.asList("bucket1,measurement1", "bucket1,measurement2")), BucketDataCache.getBucketDataKeySet());
    }

    @Test
    void removeBucketDataKey() {
        // 添加元素
        BucketDataCache.addBucketData("bucket1,measurement1", this.influxdbBucketDataEntity);
        // 获取集合，非空集合
        Assertions.assertEquals(new HashSet<>(Arrays.asList("bucket1,measurement1")), BucketDataCache.getBucketDataKeySet());
        // 删除指定队列记录
        BucketDataCache.removeBucketDataKey("bucket1,measurement1");
        // 获取集合，空集合
        Assertions.assertEquals(new HashSet<>(), BucketDataCache.getBucketDataKeySet());
    }
}