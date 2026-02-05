package com.taosdata.example.mybatisplusdemo.provider;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
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
 * <p>Originally designed for TDengine time-series database, but applicable
 * to any JDBC database with batch support (MySQL, PostgreSQL, Oracle, etc.).</p>
 *
 * <p><b>IMPORTANT</b>: This writer is a Spring singleton bean.</p>
 *
 * <p><b>Usage Pattern:</b></p>
 * <pre>{@code
 * @Autowired
 * private CachedBatchWriter writer;
 *
 * public void saveData(List<Meters> data) {
 *     writer.fastBatchWrite("com.example.mapper.MetersMapper.insert", data);
 * }
 * }</pre>
 *
 * <p><b>PreparedStatement Caching Strategy:</b></p>
 * <ul>
 *   <li>Each thread caches its own Connections and PreparedStatements</li>
 *   <li>Key: {@code threadId + ":" + msId}</li>
 *   <li>Cleanup: On application shutdown via {@link DisposableBean}</li>
 * </ul>
 *
 * <p><b>Connection Pool Configuration:</b></p>
 * If your application has N threads processing data, configure Druid:
 * <pre>{@code
 * spring.datasource.druid.max-active: ${N * average_sql_count_per_thread}
 * }</pre>
 *
 * @see SqlSessionFactory
 * @see DataSource
 */
@Component
public class CachedBatchWriter implements DisposableBean {

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

    /**
     * Creates a new writer instance.
     *
     * @param sqlSessionFactory MyBatis SqlSessionFactory
     * @param dataSource JDBC DataSource
     */
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
     * <p>On first call for current thread, creates Connection and PreparedStatement.
     * On subsequent calls, reuses existing PreparedStatement.</p>
     *
     * @param msId MyBatis Mapper method ID in format "namespace.methodId"
     * @param dataList list of data to insert
     * @throws RuntimeException if batch write fails
     */
    public <T> void fastBatchWrite(String msId, List<T> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        String cacheKey = getCacheKey(msId);
        StatementCache sc = cache.get(cacheKey);

        try {
            // Check if we need to create PreparedStatement
            if (sc == null || sc.ps.isClosed() || sc.ps.getConnection().isClosed()) {
                // Clean up old PreparedStatement if exists
                if (sc != null && !sc.ps.isClosed()) {
                    closeCache(sc);
                }

                sc = createStatementCache(msId, dataList.get(0));
                cache.put(cacheKey, sc);
                psCreateCount.incrementAndGet();
            }

            PreparedStatement ps = sc.ps;

            // Synchronize on StatementCache to prevent concurrent execution
            // on the same PreparedStatement within the same thread
            synchronized (sc) {
                for (T item : dataList) {
                    BoundSql boundSql = sc.ms.getBoundSql(item);
                    sc.conf.newParameterHandler(sc.ms, item, boundSql).setParameters(ps);
                    ps.addBatch();
                }

                ps.executeBatch();
                ps.clearBatch();
                sc.conn.commit();
            }

        } catch (SQLException e) {
            if (isConnectionError(e)) {
                closeCache(sc);
                cache.remove(cacheKey);
            }
            throw new RuntimeException("Batch write failed", e);
        }
    }

    /**
     * Gets PreparedStatement creation count.
     *
     * @return number of times PreparedStatement was created
     */
    public long getPsCreateCount() {
        return psCreateCount.get();
    }

    /**
     * Cleanup all cached PreparedStatements and Connections.
     *
     * <p>Called automatically on application shutdown via {@link DisposableBean}.</p>
     */
    @Override
    public void destroy() {
        cache.values().forEach(this::closeCache);
        cache.clear();
    }

    /**
     * Generates cache key for current thread and msId.
     *
     * @param msId MyBatis Mapper method ID
     * @return cache key in format "threadId:msId"
     */
    private String getCacheKey(String msId) {
        return Thread.currentThread().getId() + ":" + msId;
    }

    private <T> StatementCache createStatementCache(String msId, T sample) throws SQLException {
        Configuration conf = sqlSessionFactory.getConfiguration();
        MappedStatement ms = conf.getMappedStatement(msId);
        String sql = ms.getBoundSql(sample).getSql();

        Connection conn = dataSource.getConnection();
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement(sql);

        return new StatementCache(Thread.currentThread().getId(), conn, ps, ms, conf);
    }

    private boolean isConnectionError(SQLException e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("connection") || msg.contains("closed"));
    }

    private void closeCache(StatementCache sc) {
        if (sc != null && sc.conn != null) {
            try {
                // Rollback transaction before closing connection
                if (!sc.conn.getAutoCommit()) {
                    sc.conn.rollback();
                }
            } catch (Exception ignored) {}
        }
        try { if (sc != null && sc.ps != null) sc.ps.close(); } catch (Exception ignored) {}
        try { if (sc != null && sc.conn != null) sc.conn.close(); } catch (Exception ignored) {}
    }
}
