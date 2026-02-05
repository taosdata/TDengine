package com.taosdata.example.mybatisplusdemo.provider;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cached PreparedStatement batch writer for high performance.
 *
 * <p><b>Usage:</b></p>
 * <pre>{@code
 * @Autowired
 * private CachedBatchWriter writer;
 *
 * public void saveData(List<Meters> data) throws SQLException {
 *     writer.fastBatchWrite("com.example.mapper.MetersMapper.insert", data);
 * }
 * }</pre>
 *
 * <p><b>Caching Strategy:</b></p>
 * <ul>
 *   <li>Key: {@code threadId:msId}</li>
 *   <li>Each thread maintains its own PreparedStatement cache</li>
 *   <li>Use {@code @Transactional} for transaction control</li>
 * </ul>
 */
@Component
public class CachedBatchWriter implements DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(CachedBatchWriter.class);

    private final SqlSessionFactory sqlSessionFactory;
    private final DataSource dataSource;
    private final ConcurrentHashMap<String, StatementCache> cache;
    private final AtomicLong psCreateCount;

    private static class StatementCache {
        final long threadId;
        final Connection conn;
        final PreparedStatement ps;
        final MappedStatement ms;
        final Configuration conf;

        StatementCache(long threadId, Connection conn, PreparedStatement ps,
                      MappedStatement ms, Configuration conf) {
            this.threadId = threadId;
            this.conn = conn;
            this.ps = ps;
            this.ms = ms;
            this.conf = conf;
        }
    }

    @Autowired
    public CachedBatchWriter(SqlSessionFactory sqlSessionFactory, DataSource dataSource) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.dataSource = dataSource;
        this.cache = new ConcurrentHashMap<>();
        this.psCreateCount = new AtomicLong(0);
    }

    /**
     * Batch write data with cached PreparedStatement.
     *
     * @param msId MyBatis Mapper method ID in format "namespace.methodId"
     * @param dataList list of data to insert
     * @throws SQLException if batch write fails
     */
    public <T> void fastBatchWrite(String msId, List<T> dataList) throws SQLException {
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        String cacheKey = getCacheKey(msId);

        try {
            StatementCache sc = cache.get(cacheKey);

            if (sc == null || sc.ps.isClosed()) {
                StatementCache newSc = createStatementCache(msId, dataList.get(0));
                StatementCache existingSc = cache.putIfAbsent(cacheKey, newSc);
                sc = (existingSc != null) ? existingSc : newSc;

                if (existingSc == null) {
                    psCreateCount.incrementAndGet();
                    logger.debug("PreparedStatement created for msId: {}, total: {}", msId, psCreateCount.get());
                } else {
                    closeCache(newSc);
                }
            } else {
                logger.debug("Reusing cached PreparedStatement for msId: {}", msId);
            }

            PreparedStatement ps = sc.ps;

            // No synchronization needed - each thread has its own StatementCache
            for (T item : dataList) {
                BoundSql boundSql = sc.ms.getBoundSql(item);
                sc.conf.newParameterHandler(sc.ms, item, boundSql).setParameters(ps);
                ps.addBatch();
            }

            ps.executeBatch();
            ps.clearBatch();

        } catch (SQLException e) {
            StatementCache sc = cache.get(cacheKey);
            if (isConnectionError(e)) {
                logger.error("Connection error for msId: {}, removing cache", msId, e);
                if (sc != null) {
                    closeCache(sc);
                }
                cache.remove(cacheKey);
            } else {
                logger.error("SQL error for msId: {}, size: {}", msId, dataList.size(), e);
            }
            throw e;
        }
    }

    public long getPsCreateCount() {
        return psCreateCount.get();
    }

    @Override
    public void destroy() {
        logger.info("Closing {} cached PreparedStatements", cache.size());
        cache.values().forEach(this::closeCache);
        cache.clear();
        logger.info("CachedBatchWriter destroyed. Total created: {}", psCreateCount.get());
    }

    private String getCacheKey(String msId) {
        return Thread.currentThread().getId() + ":" + msId;
    }

    private <T> StatementCache createStatementCache(String msId, T sample) throws SQLException {
        Configuration conf = sqlSessionFactory.getConfiguration();
        MappedStatement ms = conf.getMappedStatement(msId);
        String sql = ms.getBoundSql(sample).getSql();

        Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);

        return new StatementCache(Thread.currentThread().getId(), conn, ps, ms, conf);
    }

    private boolean isConnectionError(SQLException e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("connection") || msg.contains("closed"));
    }

    private void closeCache(StatementCache sc) {
        if (sc == null) {
            return;
        }

        try {
            if (sc.ps != null) {
                sc.ps.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close PreparedStatement for thread {}", sc.threadId, e);
        }

        try {
            if (sc.conn != null) {
                sc.conn.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close Connection for thread {}", sc.threadId, e);
        }
    }
}
