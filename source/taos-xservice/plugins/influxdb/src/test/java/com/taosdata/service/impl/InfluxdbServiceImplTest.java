package com.taosdata.service.impl;

import com.taosdata.config.InfluxdbConfig;
import com.taosdata.utils.exception.ArtificialException;
import com.taosdata.utils.influxdbV1.InfluxdbV1ClientPool;
import com.taosdata.utils.influxdbV1.InfluxdbV1PoolAutoConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InfluxdbServiceImplTest {

    @InjectMocks
    InfluxdbServiceImpl influxdbServiceImpl;

    @Mock
    InfluxdbV1PoolAutoConfig influxdbV1Pool;

    @Mock
    InfluxdbV1ClientPool influxdbV1ClientPool;

    @Mock
    InfluxDB influxDB;

    @Mock
    InfluxdbConfig influxdbConfig;

    @Test
    void testEscapeBackslash() {
        Assertions.assertEquals("=", influxdbServiceImpl.escapeBackslash("\\="));
        Assertions.assertEquals(",", influxdbServiceImpl.escapeBackslash("\\,"));
        Assertions.assertEquals(" ", influxdbServiceImpl.escapeBackslash("\\ "));
        Assertions.assertEquals("\\\\", influxdbServiceImpl.escapeBackslash("\\"));
        Assertions.assertEquals("\\\\=", influxdbServiceImpl.escapeBackslash("\\\\="));
        Assertions.assertEquals("\\\\ ", influxdbServiceImpl.escapeBackslash("\\\\ "));
        Assertions.assertEquals("z\\\\\"a", influxdbServiceImpl.escapeBackslash("z\\\"a"));
        Assertions.assertEquals("zgc abc", influxdbServiceImpl.escapeBackslash("zgc\\ abc"));
        Assertions.assertEquals("zgc\\\\ abc", influxdbServiceImpl.escapeBackslash("zgc\\\\ abc"));
    }

    @Test
    void getTagSetPage_shouldEscapeIdentifierAndReturnRawCount() throws Exception {
        when(influxdbV1Pool.getPool()).thenReturn(influxdbV1ClientPool);
        when(influxdbV1ClientPool.borrowObject()).thenReturn(influxDB);
        when(influxDB.query(any(Query.class))).thenReturn(buildShowSeriesResult("cpu,host=server\\,1,region=cn\\=north"));

        Pair<List<List<Pair<String, String>>>, Long> page = influxdbServiceImpl.getTagSetPage("bucket", "cpu\"x", 100L, 20L);

        Assertions.assertEquals(1L, page.getRight());
        Assertions.assertEquals(1, page.getLeft().size());
        Assertions.assertEquals(Pair.of("host", "server,1"), page.getLeft().get(0).get(0));
        Assertions.assertEquals(Pair.of("region", "cn=north"), page.getLeft().get(0).get(1));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(influxDB).query(queryCaptor.capture());
        Assertions.assertEquals("show series from \"cpu\\\"x\" limit 100 offset 20", queryCaptor.getValue().getCommand());
        verify(influxdbV1ClientPool).returnObject(influxDB);
    }

    @Test
    void getTagSetPage_shouldReturnEmptyWhenQueryResultIsNull() throws Exception {
        when(influxdbV1Pool.getPool()).thenReturn(influxdbV1ClientPool);
        when(influxdbV1ClientPool.borrowObject()).thenReturn(influxDB);
        when(influxDB.query(any(Query.class))).thenReturn(null);

        Pair<List<List<Pair<String, String>>>, Long> page = influxdbServiceImpl.getTagSetPage("bucket", "measurement", 100L, 0L);

        Assertions.assertTrue(page.getLeft().isEmpty());
        Assertions.assertEquals(0L, page.getRight());
        verify(influxdbV1ClientPool).returnObject(influxDB);
    }

    @Test
    void getTagSetPage_shouldWrapPoolException() throws Exception {
        when(influxdbV1Pool.getPool()).thenReturn(influxdbV1ClientPool);
        when(influxdbV1ClientPool.borrowObject()).thenThrow(new RuntimeException("boom"));

        Assertions.assertThrows(ArtificialException.class, () -> influxdbServiceImpl.getTagSetPage("bucket", "cpu", 1L, 0L));

        verify(influxdbV1ClientPool, never()).returnObject(any(InfluxDB.class));
    }

    @Test
    void getFirstTimestampInRange_shouldUseValidInfluxqlForV1() throws Exception {
        when(influxdbConfig.getVersion()).thenReturn("1.7");
        when(influxdbV1Pool.getPool()).thenReturn(influxdbV1ClientPool);
        when(influxdbV1ClientPool.borrowObject()).thenReturn(influxDB);
        when(influxDB.query(any(Query.class))).thenReturn(buildFirstTimestampResult("2026-05-01T00:00:00Z"));

        Instant firstTimestamp = influxdbServiceImpl.getFirstTimestampInRange("org", "bucket", "cpu\"x", "2026-05-01T00:00:00Z");

        Assertions.assertEquals(Instant.parse("2026-05-01T00:00:00Z"), firstTimestamp);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(influxDB).query(queryCaptor.capture());
        Assertions.assertEquals(
                "select * from \"cpu\\\"x\" where time >= '2026-05-01T00:00:00Z' order by time asc limit 1",
                queryCaptor.getValue().getCommand()
        );
        verify(influxdbV1ClientPool).returnObject(influxDB);
    }

    @Test
    void getFirstTimestampInRange_shouldKeepCurrentBehaviorWhenQueryResultHasError() throws Exception {
        when(influxdbConfig.getVersion()).thenReturn("1.7");
        when(influxdbV1Pool.getPool()).thenReturn(influxdbV1ClientPool);
        when(influxdbV1ClientPool.borrowObject()).thenReturn(influxDB);
        QueryResult queryResult = new QueryResult();
        queryResult.setError("parse error");
        when(influxDB.query(any(Query.class))).thenReturn(queryResult);

        Instant firstTimestamp = influxdbServiceImpl.getFirstTimestampInRange("org", "bucket", "cpu", "2026-05-01T00:00:00Z");

        Assertions.assertNull(firstTimestamp);
        verify(influxdbV1ClientPool).returnObject(influxDB);
    }

    private QueryResult buildShowSeriesResult(String... lines) {
        return buildShowSeriesResult(Arrays.asList(lines));
    }

    private QueryResult buildShowSeriesResult(List<String> lines) {
        QueryResult.Series series = new QueryResult.Series();
        List<List<Object>> values = new ArrayList<>();
        for (String line : lines) {
            values.add(Collections.<Object>singletonList(line));
        }
        series.setValues(values);

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Collections.singletonList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Collections.singletonList(result));
        return queryResult;
    }

    private QueryResult buildFirstTimestampResult(String timestamp) {
        QueryResult.Series series = new QueryResult.Series();
        series.setColumns(Arrays.asList("time", "value"));
        series.setValues(Collections.singletonList(Arrays.<Object>asList(timestamp, 1)));

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Collections.singletonList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Collections.singletonList(result));
        return queryResult;
    }
}
