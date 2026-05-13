package com.taosdata.taosx.pspace.ipc;

import com.taosdata.taosx.pspace.PointResolver.ResolvedPoints;
import com.taosdata.taosx.pspace.RawDataLogger;
import com.taosdata.taosx.pspace.config.AdvancedOptionsConfig;
import com.taosdata.taosx.pspace.config.Configuration;
import com.taosdata.taosx.pspace.config.ReportConfig;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Groups data points by their pSpace data type and manages one
 * {@link PointStreamWriter} per group (one TCP connection per Arrow schema).
 * <p>
 * Points whose data type is unknown or unset are placed in the default
 * DOUBLE group.
 *
 * <h3>Lifecycle</h3>
 *
 * <pre>
 * DataTypeGroup g = DataTypeGroup.create(reportConfig, resolvedPoints);
 * g.connect();                        // opens all TCP connections
 * g.writeData(tagId, name, ts, ...);  // routes to correct writer
 * g.flushAll();
 * g.close();
 * </pre>
 */
public class DataTypeGroup implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(DataTypeGroup.class);

    /** Maps groupKey → writer. */
    private final Map<String, PointStreamWriter> writers = new LinkedHashMap<>();

    /** Maps tagId → groupKey for data routing. */
    private final Map<Long, String> tagGroupMap = new HashMap<>();

    private final String remote;
    private final int batchSize;
    private final boolean localOnly;
    private RawDataLogger rawDataLogger;

    private DataTypeGroup(String remote, int batchSize, boolean localOnly, RawDataLogger rawDataLogger) {
        this.remote = remote;
        this.batchSize = batchSize;
        this.localOnly = localOnly;
        this.rawDataLogger = rawDataLogger;
    }

    /**
     * Create a DataTypeGroup from configuration and resolved points.
     *
     * @param cfg    full configuration (report for IPC, advanced_options for
     *               batch_size and raw data settings)
     * @param points resolved point metadata (includes data types)
     * @return configured group (not yet connected)
     */
    public static DataTypeGroup create(Configuration cfg, ResolvedPoints points) throws IOException {
        ReportConfig reportConfig = cfg.getReport();
        AdvancedOptionsConfig advOpts = cfg.getAdvancedOptions();

        boolean localOnly = reportConfig != null && reportConfig.isLocalOnly();
        String remote = reportConfig != null ? reportConfig.getRemote() : null;

        // batch_size: read from advanced_options, default 1000
        int batchSize = 1000;
        if (advOpts != null && advOpts.getBatchSize() != null) {
            batchSize = advOpts.getBatchSize().intValue();
        }

        // Setup raw data logger from advanced_options
        RawDataLogger rawLog = null;
        boolean keepRaw = advOpts != null && advOpts.isKeepRawData();
        if (keepRaw || localOnly) {
            String dir = (advOpts != null && advOpts.getKeepRawDataDir() != null)
                    ? advOpts.getKeepRawDataDir()
                    : "./rawdata";
            rawLog = new RawDataLogger(dir);
        }

        DataTypeGroup group = new DataTypeGroup(remote, batchSize, localOnly, rawLog);

        // Group points by data type
        Map<String, List<Long>> grouped = new LinkedHashMap<>();
        for (Long tagId : points.getTagIds()) {
            String psType = points.getDataType(tagId);
            String key = DataTypeMapper.toGroupKey(psType);
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(tagId);
        }

        // Create one writer per group (skip if local_only)
        for (Map.Entry<String, List<Long>> entry : grouped.entrySet()) {
            String key = entry.getKey();
            List<Long> tagIds = entry.getValue();

            // Map tagIds to this group
            for (Long tagId : tagIds) {
                group.tagGroupMap.put(tagId, key);
            }

            if (!localOnly) {
                String psType = points.getTypeMap().entrySet().stream()
                        .filter(e -> DataTypeMapper.toGroupKey(e.getValue()).equals(key))
                        .map(Map.Entry::getValue)
                        .findFirst()
                        .orElse(null);
                ArrowType arrowType = DataTypeMapper.toArrowType(psType);
                PointStreamWriter writer = new PointStreamWriter(remote, arrowType, batchSize);
                group.writers.put(key, writer);
            }

            logger.info("Data type group '{}': {} points", key, tagIds.size());
        }

        // If no points have data type info, ensure at least one DEFAULT group
        if (grouped.isEmpty() && !localOnly) {
            PointStreamWriter writer = new PointStreamWriter(
                    remote, DataTypeMapper.DEFAULT_ARROW_TYPE, batchSize);
            group.writers.put(DataTypeMapper.DEFAULT_TYPE_KEY, writer);
            logger.info("No data type info; using default DOUBLE group");
        }

        return group;
    }

    /** Open all TCP connections. */
    public void connect() throws IOException {
        for (Map.Entry<String, PointStreamWriter> entry : writers.entrySet()) {
            logger.info("Connecting data type group '{}' to {}", entry.getKey(), remote);
            entry.getValue().connect();
        }
    }

    /**
     * Write a data row, routing to the correct stream writer based on tagId.
     */
    public void writeData(long tagId, String name, long tsMs, long receivedMs,
            Object value, long status, long requestMs) throws IOException {
        // Raw data logging
        if (rawDataLogger != null) {
            rawDataLogger.write(tagId, name, tsMs, value, (int) status);
        }

        // Skip IPC if local_only
        if (localOnly)
            return;

        String key = tagGroupMap.getOrDefault(tagId, DataTypeMapper.DEFAULT_TYPE_KEY);
        PointStreamWriter writer = writers.get(key);
        if (writer == null) {
            // Fallback: use any available writer (shouldn't happen if points are properly
            // resolved)
            writer = writers.values().iterator().next();
            logger.warn("No writer for tagId={} group={}, using fallback", tagId, key);
        }
        writer.writeData(String.valueOf(tagId), name, tsMs, receivedMs, value, status, requestMs);
    }

    /** Flush all writers. */
    public void flushAll() throws IOException {
        for (PointStreamWriter writer : writers.values()) {
            writer.flushBatch();
        }
        if (rawDataLogger != null) {
            rawDataLogger.flush();
        }
    }

    /** Close all writers and raw data logger. */
    @Override
    public void close() {
        for (Map.Entry<String, PointStreamWriter> entry : writers.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.warn("Error closing writer for group '{}': {}", entry.getKey(), e.getMessage());
            }
        }
        writers.clear();
        if (rawDataLogger != null) {
            rawDataLogger.close();
            rawDataLogger = null;
        }
    }

    public boolean isLocalOnly() {
        return localOnly;
    }

    /**
     * Assign a tagId to the default group (used when data type is unknown at
     * resolve time).
     */
    public void ensureTagGroup(long tagId) {
        tagGroupMap.putIfAbsent(tagId, DataTypeMapper.DEFAULT_TYPE_KEY);
    }

    public Set<String> getGroupKeys() {
        return Collections.unmodifiableSet(writers.keySet());
    }
}
