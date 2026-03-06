package com.taosdata.taosx.pspace.run;

import com.sunwayland.pspace.PSpaceClient;
import com.taosdata.taosx.pspace.PointResolver;
import com.taosdata.taosx.pspace.PointResolver.ResolvedPoints;
import com.taosdata.taosx.pspace.config.Configuration;
import com.taosdata.taosx.pspace.config.RunConfig;
import com.taosdata.taosx.pspace.config.TimeUtils;
import com.taosdata.taosx.pspace.ipc.DataTypeGroup;
import com.taosdata.taosx.pspace.query.PSpaceQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Implements the "Query" run mode: one-shot historical data migration.
 * <p>
 * Splits the {@code [start_time, end_time)} range into time windows,
 * queries pSpace for each window using the hisReadRawAll algorithm,
 * serializes to Arrow IPC, and sends via TCP to taosX.
 */
public class QueryTask {

    private static final Logger logger = LoggerFactory.getLogger(QueryTask.class);
    private static final long DEFAULT_TIME_WINDOW_SEC = 86400L; // 1 day
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private static String fmtMs(long epochMs) {
        return TIME_FMT.format(Instant.ofEpochMilli(epochMs));
    }

    public int execute(Configuration cfg) throws Exception {
        // --- Parse run config ---
        RunConfig runCfg = cfg.getRun();
        if (runCfg == null || runCfg.getStartTime() == null) {
            logger.error("Query mode requires [run].start_time");
            return -1;
        }

        long startMs = TimeUtils.parseToEpochMillis(runCfg.getStartTime());
        long endMs = runCfg.getEndTime() != null
                ? TimeUtils.parseToEpochMillis(runCfg.getEndTime())
                : System.currentTimeMillis();
        long windowSec = runCfg.getTimeWindow() != null ? runCfg.getTimeWindow() : DEFAULT_TIME_WINDOW_SEC;
        long windowMs = windowSec * 1000L;

        logger.info("Query mode: start={}, end={}, window={}s",
                fmtMs(startMs), fmtMs(endMs), windowSec);

        // --- Connect to pSpace ---
        PSpaceClient client = cfg.tryConnect();

        // --- Resolve points ---
        ResolvedPoints resolvedPoints = PointResolver.resolve(cfg, client);
        logger.info("Resolved {} points", resolvedPoints.size());

        // --- Setup IPC ---
        DataTypeGroup group = DataTypeGroup.create(cfg, resolvedPoints);
        if (!group.isLocalOnly()) {
            group.connect();
        }

        try {
            // --- Create query executor ---
            PSpaceQueryExecutor executor = new PSpaceQueryExecutor(client, resolvedPoints, group);

            // --- Split into time windows and query ---
            long totalRows = 0;
            long windowStart = startMs;
            int windowIdx = 0;

            while (windowStart < endMs) {
                long windowEnd = Math.min(windowStart + windowMs, endMs);
                long queryStart = windowStart;

                windowIdx++;
                logger.info("Window {}: query [{}, {})", windowIdx, fmtMs(queryStart), fmtMs(windowEnd));

                long rows = executor.queryRange(queryStart, windowEnd);
                totalRows += rows;
                logger.info("Window {} completed: {} rows (total: {})", windowIdx, rows, totalRows);

                windowStart = windowEnd;
            }

            group.flushAll();
            logger.info("Query mode completed: {} total rows across {} windows", totalRows, windowIdx);
            return 0;
        } finally {
            group.close();
            try {
                client.disconnect();
            } catch (Exception e) {
                /* ignore */ }
        }
    }
}
