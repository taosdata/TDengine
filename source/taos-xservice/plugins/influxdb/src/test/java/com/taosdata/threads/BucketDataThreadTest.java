package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.dto.ThreadConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.service.impl.InfluxdbServiceImpl;
import com.taosdata.utils.exception.ArtificialException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BucketDataThreadTest {

    private InfluxdbServiceImpl influxdbService;

    @BeforeEach
    void setUp() {
        clearBucketDataCache();
        StatisticCache.completedTaskSet.clear();
        StatisticCache.totalRead.set(0L);
        LocalConfig.isRunBucketDataThread = true;

        PerformanceConfig performanceConfig = new PerformanceConfig();
        ThreadConfig threadConfig = new ThreadConfig();
        threadConfig.setReadBucketInterval(0L);
        threadConfig.setReadBucketFullInterval(0L);
        performanceConfig.setThread(threadConfig);
        performanceConfig.setQueueSizeD(1000L);

        influxdbService = mock(InfluxdbServiceImpl.class);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBean(PerformanceConfig.class)).thenReturn(performanceConfig);
        when(applicationContext.getBean(InfluxdbServiceImpl.class)).thenReturn(influxdbService);
        new ApplicationContextProvider().setApplicationContext(applicationContext);
    }

    @AfterEach
    void tearDown() {
        LocalConfig.isRunBucketDataThread = true;
        clearBucketDataCache();
        StatisticCache.completedTaskSet.clear();
        StatisticCache.totalRead.set(0L);
    }

    @Test
    void run_shouldPaginateTagSetUntilRawSeriesCountIsZero() throws Exception {
        BucketDataThread thread = new BucketDataThread("org", "bucket", "measurement", "start", "stop");
        when(influxdbService.getInfluxdbVersion()).thenReturn("1.8");
        when(influxdbService.selectAllFields("bucket", "measurement")).thenReturn(Collections.singletonMap("field", "double"));
        when(influxdbService.getTagSetPage("bucket", "measurement", 10000L, 0L))
                .thenReturn(Pair.of(Collections.singletonList(Collections.singletonList(Pair.of("host", "a"))), 10000L));
        when(influxdbService.getTagSetPage("bucket", "measurement", 10000L, 10000L))
                .thenReturn(Pair.of(Collections.<List<Pair<String, String>>>emptyList(), 0L));
        when(influxdbService.selectBucketDataV1(eq("bucket"), eq("measurement"), anyString(), eq("start"), eq("stop"), anyLong(), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList());

        thread.run();

        verify(influxdbService).getTagSetPage("bucket", "measurement", 10000L, 0L);
        verify(influxdbService).getTagSetPage("bucket", "measurement", 10000L, 10000L);
        verify(influxdbService).selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='a'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(-1L));
        Assertions.assertTrue(StatisticCache.completedTaskSet.contains("bucket,measurement,start,stop"));
    }

    @Test
    void run_shouldBreakWhenPageIsNotFull() throws Exception {
        BucketDataThread thread = new BucketDataThread("org", "bucket", "measurement", "start", "stop");
        when(influxdbService.getInfluxdbVersion()).thenReturn("1.8");
        when(influxdbService.selectAllFields("bucket", "measurement")).thenReturn(Collections.singletonMap("field", "double"));
        when(influxdbService.getTagSetPage("bucket", "measurement", 10000L, 0L))
                .thenReturn(Pair.of(Collections.singletonList(Collections.singletonList(Pair.of("host", "b"))), 1L));
        when(influxdbService.selectBucketDataV1(eq("bucket"), eq("measurement"), anyString(), eq("start"), eq("stop"), anyLong(), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList());

        thread.run();

        verify(influxdbService, times(1)).getTagSetPage("bucket", "measurement", 10000L, 0L);
        verify(influxdbService, never()).getTagSetPage("bucket", "measurement", 10000L, 10000L);
        Assertions.assertTrue(StatisticCache.completedTaskSet.contains("bucket,measurement,start,stop"));
    }

    @Test
    void run_shouldStoreLastTimestampWhenEntityListIsNotEmpty() throws Exception {
        BucketDataThread thread = new BucketDataThread("org", "bucket", "measurement", "start", "stop");
        when(influxdbService.getInfluxdbVersion()).thenReturn("1.8");
        when(influxdbService.selectAllFields("bucket", "measurement")).thenReturn(Collections.singletonMap("field", "double"));
        when(influxdbService.getTagSetPage("bucket", "measurement", 10000L, 0L))
                .thenReturn(Pair.of(Collections.singletonList(Collections.singletonList(Pair.of("host", "c"))), 1L));
        when(influxdbService.selectBucketDataV1(eq("bucket"), eq("measurement"), anyString(), eq("start"), eq("stop"), anyLong(), anyLong(), anyLong()))
                .thenAnswer(invocation -> {
                    InfluxdbBucketDataEntity entity = new InfluxdbBucketDataEntity();
                    entity.setTime(Instant.ofEpochSecond(1700000000L, 123L));
                    LocalConfig.isRunBucketDataThread = false;
                    return Collections.singletonList(entity);
                });

        thread.run();

        verify(influxdbService).selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='c'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(-1L));
    }

    @Test
    void run_shouldOnlyQueryActiveTagSetAfterFirstScan() throws Exception {
        BucketDataThread thread = new BucketDataThread("org", "bucket", "measurement", "start", "stop");
        when(influxdbService.getInfluxdbVersion()).thenReturn("1.8");
        when(influxdbService.selectAllFields("bucket", "measurement")).thenReturn(Collections.singletonMap("field", "double"));

        List<List<Pair<String, String>>> tagSetPage = Arrays.asList(
                Collections.singletonList(Pair.of("host", "a")),
                Collections.singletonList(Pair.of("host", "b"))
        );
        when(influxdbService.getTagSetPage("bucket", "measurement", 10000L, 0L))
                .thenReturn(Pair.of(tagSetPage, 2L));

        InfluxdbBucketDataEntity entity = new InfluxdbBucketDataEntity();
        entity.setTime(Instant.ofEpochSecond(1700000001L, 456L));
        long expectedLastTime = entity.getTime().getEpochSecond() * 1000_000_000L + entity.getTime().getNano();

        when(influxdbService.selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='a'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(-1L)))
                .thenReturn(Collections.emptyList());
        when(influxdbService.selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='b'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(-1L)))
                .thenReturn(Collections.singletonList(entity));
        when(influxdbService.selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='b'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(expectedLastTime)))
                .thenReturn(Collections.emptyList());

        thread.run();

        verify(influxdbService, times(2)).getTagSetPage("bucket", "measurement", 10000L, 0L);
        verify(influxdbService, times(1)).selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='a'"), eq("start"), eq("stop"), anyLong(), anyLong(), anyLong());
        verify(influxdbService, times(1)).selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='b'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(-1L));
        verify(influxdbService, times(1)).selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='b'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(expectedLastTime));
        Assertions.assertTrue(StatisticCache.completedTaskSet.contains("bucket,measurement,start,stop"));
    }

    @Test
    void run_shouldCatchArtificialExceptionFromSelectBucketDataV1() throws Exception {
        BucketDataThread thread = new BucketDataThread("org", "bucket", "measurement", "start", "stop");
        when(influxdbService.getInfluxdbVersion()).thenReturn("1.8");
        when(influxdbService.selectAllFields("bucket", "measurement")).thenReturn(Collections.singletonMap("field", "double"));
        when(influxdbService.getTagSetPage("bucket", "measurement", 10000L, 0L))
                .thenReturn(Pair.of(Collections.singletonList(Collections.singletonList(Pair.of("host", "d"))), 1L));
        when(influxdbService.selectBucketDataV1(eq("bucket"), eq("measurement"), anyString(), eq("start"), eq("stop"), anyLong(), anyLong(), anyLong()))
                .thenThrow(new ArtificialException("ERR", "failed", new Exception("boom")));

        thread.run();

        verify(influxdbService).selectBucketDataV1(eq("bucket"), eq("measurement"), eq("\"host\"='d'"), eq("start"), eq("stop"), anyLong(), anyLong(), eq(-1L));
        Assertions.assertTrue(StatisticCache.completedTaskSet.contains("bucket,measurement,start,stop"));
    }

    private void clearBucketDataCache() {
        BucketDataCache.getBucketData(BucketDataCache.getBucketDataQueueSize());
        String[] keys = BucketDataCache.getBucketDataKeySet().toArray(new String[0]);
        for (String key : keys) {
            BucketDataCache.removeBucketDataKey(key);
        }
    }
}
