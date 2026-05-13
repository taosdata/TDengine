package com.taosdata.caches;

import com.taosdata.model.entity.OpentsdbDataEntity;
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
class MetricDataCacheTest {

    @MockBean
    OpentsdbDataEntity opentsdbDataEntity;

    @BeforeEach
    void clearBucketDataCache() {
        MetricDataCache.getMetricData(MetricDataCache.getMetricDataQueueSize());
        MetricDataCache.getMetricDataKeySet().forEach(key -> MetricDataCache.removeMetricDataKey(key));
    }

    @Test
    void addMetricData_List() {
        // 添加元素，队列长度 2
        Assertions.assertEquals(2, MetricDataCache.addMetricData(Arrays.asList(this.opentsdbDataEntity, this.opentsdbDataEntity)));
    }

    @Test
    void addMetricData_ByKey() {
        // 添加元素，队列长度 1
        Assertions.assertEquals(1, MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity));
        // 添加元素，队列长度 2
        Assertions.assertEquals(2, MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity));
    }

    @Test
    void getMetricData() {
        // 取到队列，空队列
        Assertions.assertEquals(new ArrayList<>(), MetricDataCache.getMetricData(10));
        // 添加元素
        MetricDataCache.addMetricData(Arrays.asList(this.opentsdbDataEntity, this.opentsdbDataEntity));
        // 取到队列，非空队列
        Assertions.assertEquals(Arrays.asList(this.opentsdbDataEntity, this.opentsdbDataEntity), MetricDataCache.getMetricData(10));
    }

    @Test
    void getMetricData_ByKey() {
        // 取到队列，空队列
        Assertions.assertEquals(new ArrayList<>(), MetricDataCache.getMetricData("metric1", 10));
        // 添加元素
        MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity);
        MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity);
        // 取到队列，队列长度为2
        Assertions.assertEquals(Arrays.asList(this.opentsdbDataEntity, this.opentsdbDataEntity), MetricDataCache.getMetricData("metric1", 10));
    }

    @Test
    void getMetricDataQueueSize() {
        // 获取队列长度 0
        Assertions.assertEquals(0, MetricDataCache.getMetricDataQueueSize());
        // 添加元素
        MetricDataCache.addMetricData(Arrays.asList(this.opentsdbDataEntity));
        // 获取队列长度 1
        Assertions.assertEquals(1, MetricDataCache.getMetricDataQueueSize());
        // 添加元素
        MetricDataCache.addMetricData(Arrays.asList(this.opentsdbDataEntity));
        // 获取队列长度 2
        Assertions.assertEquals(2, MetricDataCache.getMetricDataQueueSize());
    }

    @Test
    void getMetricDataQueueSize_ByKey() {
        // 获取队列长度 0
        Assertions.assertEquals(0, MetricDataCache.getMetricDataQueueSize("metric1"));
        // 添加元素
        MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity);
        // 获取队列长度 1
        Assertions.assertEquals(1, MetricDataCache.getMetricDataQueueSize("metric1"));
        // 添加元素
        MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity);
        // 获取队列长度 2
        Assertions.assertEquals(2, MetricDataCache.getMetricDataQueueSize("metric1"));
    }

    @Test
    void getMetricDataQueueTotalSize() {
        // 获取队列长度 0
        Assertions.assertEquals(0, MetricDataCache.getMetricDataQueueTotalSize());
        // 添加元素
        MetricDataCache.addMetricData(Arrays.asList(this.opentsdbDataEntity));
        // 获取队列长度 1
        Assertions.assertEquals(1, MetricDataCache.getMetricDataQueueTotalSize());
        // 添加元素
        MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity);
        // 获取队列长度 2
        Assertions.assertEquals(2, MetricDataCache.getMetricDataQueueTotalSize());
    }

    @Test
    void getMetricDataKeySet() {
        // 获取集合，空集合
        Assertions.assertEquals(new HashSet<>(), MetricDataCache.getMetricDataKeySet());
        // 添加元素
        MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity);
        MetricDataCache.addMetricData("metric2", this.opentsdbDataEntity);
        // 获取集合，非空集合
        Assertions.assertEquals(new HashSet<>(Arrays.asList("metric1", "metric2")), MetricDataCache.getMetricDataKeySet());
    }

    @Test
    void removeBucketDataKey() {
        // 添加元素
        MetricDataCache.addMetricData("metric1", this.opentsdbDataEntity);
        // 获取集合，非空集合
        Assertions.assertEquals(new HashSet<>(Arrays.asList("metric1")), MetricDataCache.getMetricDataKeySet());
        // 删除指定队列记录
        MetricDataCache.removeMetricDataKey("metric1");
        // 获取集合，空集合
        Assertions.assertEquals(new HashSet<>(), MetricDataCache.getMetricDataKeySet());
    }
}