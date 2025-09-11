package com.taosdata.caches;

import com.taosdata.threads.BucketDataThread;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
class BucketCacheTest {

    @MockBean
    BucketDataThread bucketDataThread;

    @BeforeEach
    void clearBucketCache() {
        while (BucketCache.getBucketDataThread("bucket1,measurement1") != null) {
        }
        while (BucketCache.getBucketDataThread("bucket1,measurement2") != null) {
        }
        BucketCache.releaseBucketDataThreadBlocked("bucket1,measurement1");
        BucketCache.releaseBucketDataThreadBlocked("bucket1,measurement2");
    }

    @Test
    void addBucketDataThread() {
        // 添加元素，队列长度 1
        Assertions.assertEquals(1, BucketCache.addBucketDataThread("bucket1,measurement1", this.bucketDataThread));
        // 添加元素，队列长度 2
        Assertions.assertEquals(2, BucketCache.addBucketDataThread("bucket1,measurement1", this.bucketDataThread));
    }

    @Test
    void getBucketDataThread() {
        // 获取元素 null
        Assertions.assertEquals(null, BucketCache.getBucketDataThread("bucket1,measurement1"));
        // 添加元素
        BucketCache.addBucketDataThread("bucket1,measurement1", this.bucketDataThread);
        // 获取元素 this.bucketDataThread
        Assertions.assertEquals(this.bucketDataThread, BucketCache.getBucketDataThread("bucket1,measurement1"));
    }

    @Test
    void getBucketDataThreadQueueSize() {
        // 获取队列长度 0
        Assertions.assertEquals(0, BucketCache.getBucketDataThreadQueueSize("bucket1,measurement1"));
        // 添加元素
        BucketCache.addBucketDataThread("bucket1,measurement1", this.bucketDataThread);
        // 获取队列长度 1
        Assertions.assertEquals(1, BucketCache.getBucketDataThreadQueueSize("bucket1,measurement1"));
        // 添加元素
        BucketCache.addBucketDataThread("bucket1,measurement1", this.bucketDataThread);
        // 获取队列长度 2
        Assertions.assertEquals(2, BucketCache.getBucketDataThreadQueueSize("bucket1,measurement1"));
    }

    @Test
    void getBucketDataThreadQueueTotal() {
        // 获取队列长度 0
        Assertions.assertEquals(0, BucketCache.getBucketDataThreadQueueTotal());
        // 添加元素
        BucketCache.addBucketDataThread("bucket1,measurement1", this.bucketDataThread);
        // 获取队列长度 1
        Assertions.assertEquals(1, BucketCache.getBucketDataThreadQueueTotal());
        // 添加元素
        BucketCache.addBucketDataThread("bucket1,measurement1", this.bucketDataThread);
        // 获取队列长度 2
        Assertions.assertEquals(2, BucketCache.getBucketDataThreadQueueTotal());
    }

    @Test
    void setBucketDataThreadBlocked() {
        // 判断阻塞标识 false
        Assertions.assertTrue(!BucketCache.isBucketDataThreadBlocked("bucket1,measurement1"));
        // 设置阻塞
        BucketCache.setBucketDataThreadBlocked("bucket1,measurement1");
        // 判断阻塞标识 true
        Assertions.assertTrue(BucketCache.isBucketDataThreadBlocked("bucket1,measurement1"));
    }

    @Test
    void releaseBucketDataThreadBlocked() {
        // 设置阻塞
        BucketCache.setBucketDataThreadBlocked("bucket1,measurement1");
        // 判断阻塞标识 true
        Assertions.assertTrue(BucketCache.isBucketDataThreadBlocked("bucket1,measurement1"));
        // 释放阻塞
        BucketCache.releaseBucketDataThreadBlocked("bucket1,measurement1");
        // 判断阻塞标识 false
        Assertions.assertTrue(!BucketCache.isBucketDataThreadBlocked("bucket1,measurement1"));
    }

    @Test
    void isBucketDataThreadBlocked() {
        // 判断阻塞标识 false
        Assertions.assertTrue(!BucketCache.isBucketDataThreadBlocked("bucket1,measurement1"));
        // 设置阻塞
        BucketCache.setBucketDataThreadBlocked("bucket1,measurement1");
        // 判断阻塞标识 true
        Assertions.assertTrue(BucketCache.isBucketDataThreadBlocked("bucket1,measurement1"));
    }

    @Test
    void getBucketDataThreadQueueBlocked() {
        // 获取所有队列阻塞大小 0
        Assertions.assertEquals(0, BucketCache.getBucketDataThreadQueueBlocked());
        // 设置阻塞
        BucketCache.setBucketDataThreadBlocked("bucket1,measurement1");
        // 获取所有队列阻塞大小 1
        Assertions.assertEquals(1, BucketCache.getBucketDataThreadQueueBlocked());
        // 设置阻塞
        BucketCache.setBucketDataThreadBlocked("bucket1,measurement2");
        // 获取所有队列阻塞大小 2
        Assertions.assertEquals(2, BucketCache.getBucketDataThreadQueueBlocked());
    }

    @Test
    void generateBucketDataThreadKey() {
        Assertions.assertEquals("bucket1,measurement1", BucketCache.generateBucketDataThreadKey("bucket1", "measurement1"));
        Assertions.assertEquals("bucket1,measurement2", BucketCache.generateBucketDataThreadKey("bucket1", "measurement2"));
    }

    @Test
    void updateQueryLimit() {
        // 获取指定measurement的读取limit，默认1
        Assertions.assertEquals(1000, BucketCache.getQueryLimit("bucket1,measurement1"));
        // 根据子表数量与列数量更新指定measurement的读取limit，高于默认使用默认，100
        BucketCache.updateQueryLimit("bucket1,measurement1", 8, 1000, 100);
        Assertions.assertEquals(100, BucketCache.getQueryLimit("bucket1,measurement1"));
        // 根据子表数量与列数量更新指定measurement的读取limit，低于默认更新，66
        BucketCache.updateQueryLimit("bucket1,measurement1", 15, 1000, 100);
        Assertions.assertEquals(66, BucketCache.getQueryLimit("bucket1,measurement1"));
        // 根据子表数量与列数量更新指定measurement的读取limit，子表变少不更新，66
        BucketCache.updateQueryLimit("bucket1,measurement1", 12, 1000, 100);
        Assertions.assertEquals(66, BucketCache.getQueryLimit("bucket1,measurement1"));
        // 根据子表数量与列数量更新指定measurement的读取limit，子表变多更新，55
        BucketCache.updateQueryLimit("bucket1,measurement1", 18, 1000, 100);
        Assertions.assertEquals(55, BucketCache.getQueryLimit("bucket1,measurement1"));
        // 根据子表数量与列数量更新指定measurement的读取limit，低于1默认1，1
        BucketCache.updateQueryLimit("bucket1,measurement1", 1001, 1000, 100);
        Assertions.assertEquals(1, BucketCache.getQueryLimit("bucket1,measurement1"));
    }

    @Test
    void getQueryLimit() {
        // 获取指定measurement的读取limit，默认1
        Assertions.assertEquals(1000, BucketCache.getQueryLimit("bucket1,measurement2"));
        // 根据子表数量与列数量更新指定measurement的读取limit
        BucketCache.updateQueryLimit("bucket1,measurement2", 15, 1000, 100);
        // 获取指定measurement的读取limit，66
        Assertions.assertEquals(66, BucketCache.getQueryLimit("bucket1,measurement2"));
    }
}