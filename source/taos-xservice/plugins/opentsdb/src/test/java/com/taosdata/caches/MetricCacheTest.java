package com.taosdata.caches;

import com.taosdata.threads.MetricDataThread;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
class MetricCacheTest {

    @MockBean
    MetricDataThread metricDataThread;

    @BeforeEach
    void clearMetricCache() {
        while (MetricCache.getMetricDataThread("metric1") != null) {
        }
        MetricCache.releaseMetricDataThreadBlocked("metric1");
    }

    @Test
    void addMetricDataThread() {
        // 添加，队列长度 1
        Assertions.assertEquals(1, MetricCache.addMetricDataThread("metric1", this.metricDataThread));
        // 添加，队列长度 2
        Assertions.assertEquals(2, MetricCache.addMetricDataThread("metric1", this.metricDataThread));
    }

    @Test
    void getMetricDataThread() {
        // 获取元素 null
        Assertions.assertEquals(null, MetricCache.getMetricDataThread("metric1"));
        // 添加
        MetricCache.addMetricDataThread("metric1", this.metricDataThread);
        // 获取元素 this.metricDataThread
        Assertions.assertEquals(this.metricDataThread, MetricCache.getMetricDataThread("metric1"));
    }

    @Test
    void getMetricDataThreadQueueSize() {
        // 获取队列长度 0
        Assertions.assertEquals(0, MetricCache.getMetricDataThreadQueueSize("metric1"));
        // 添加元素
        MetricCache.addMetricDataThread("metric1", this.metricDataThread);
        // 获取队列长度 1
        Assertions.assertEquals(1, MetricCache.getMetricDataThreadQueueSize("metric1"));
        // 添加元素
        MetricCache.addMetricDataThread("metric1", this.metricDataThread);
        // 获取队列长度 2
        Assertions.assertEquals(2, MetricCache.getMetricDataThreadQueueSize("metric1"));
    }

    @Test
    void getMetricDataThreadQueueTotal() {
        // 获取队列长度 0
        Assertions.assertEquals(0, MetricCache.getMetricDataThreadQueueTotal());
        // 添加元素
        MetricCache.addMetricDataThread("metric1", this.metricDataThread);
        // 获取队列长度 1
        Assertions.assertEquals(1, MetricCache.getMetricDataThreadQueueTotal());
        // 添加元素
        MetricCache.addMetricDataThread("metric1", this.metricDataThread);
        // 获取队列长度 2
        Assertions.assertEquals(2, MetricCache.getMetricDataThreadQueueTotal());
    }

    @Test
    void setMetricDataThreadBlocked() {
        // 判断阻塞标识 false
        Assertions.assertTrue(!MetricCache.isMetricDataThreadBlocked("metric1"));
        // 设置阻塞
        MetricCache.setMetricDataThreadBlocked("metric1");
        // 判断阻塞标识 true
        Assertions.assertTrue(MetricCache.isMetricDataThreadBlocked("metric1"));
    }

    @Test
    void releaseMetricDataThreadBlocked() {
        // 设置阻塞
        MetricCache.setMetricDataThreadBlocked("metric1");
        // 判断阻塞标识 true
        Assertions.assertTrue(MetricCache.isMetricDataThreadBlocked("metric1"));
        // 释放阻塞
        MetricCache.releaseMetricDataThreadBlocked("metric1");
        // 判断阻塞标识 false
        Assertions.assertTrue(!MetricCache.isMetricDataThreadBlocked("metric1"));
    }

    @Test
    void isMetricDataThreadBlocked() {
        // 判断阻塞标识 false
        Assertions.assertTrue(!MetricCache.isMetricDataThreadBlocked("metric1"));
        // 设置阻塞
        MetricCache.setMetricDataThreadBlocked("metric1");
        // 判断阻塞标识 true
        Assertions.assertTrue(MetricCache.isMetricDataThreadBlocked("metric1"));
    }

    @Test
    void getMetricDataThreadQueueBlocked() {
        // 获取所有队列阻塞大小 0
        Assertions.assertEquals(0, MetricCache.getMetricDataThreadQueueBlocked());
        // 设置阻塞
        MetricCache.setMetricDataThreadBlocked("metric1");
        // 获取所有队列阻塞大小 1
        Assertions.assertEquals(1, MetricCache.getMetricDataThreadQueueBlocked());
        // 设置阻塞
        MetricCache.setMetricDataThreadBlocked("metric2");
        // 获取所有队列阻塞大小 2
        Assertions.assertEquals(2, MetricCache.getMetricDataThreadQueueBlocked());
    }
}