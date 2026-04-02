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
 * Implements the "QuerySync" run mode: historical backfill + continuous sync.
 * <p>
 * Phase 1: Query all historical data from start_time to now (same as Query
 * mode).
 * Phase 2: Continuously poll pSpace at query_interval, syncing new data.
 */
public class QuerySyncTask {

    private static final Logger logger = LoggerFactory.getLogger(QuerySyncTask.class);
    private static final long DEFAULT_TIME_WINDOW_SEC = 86400L;
    private static final long DEFAULT_TIME_EXCURSION_SEC = 0L;
    private static final long DEFAULT_QUERY_INTERVAL_SEC = 10L;
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
            logger.error("QuerySync mode requires [run].start_time");
            return -1;
        }

        long startMs = TimeUtils.parseToEpochMillis(runCfg.getStartTime());
        long windowSec = runCfg.getTimeWindow() != null ? runCfg.getTimeWindow() : DEFAULT_TIME_WINDOW_SEC;
        long excursionSec = runCfg.getTimeExcursion() != null ? runCfg.getTimeExcursion() : DEFAULT_TIME_EXCURSION_SEC;
        long queryIntervalSec = runCfg.getQueryInterval() != null ? runCfg.getQueryInterval()
                : DEFAULT_QUERY_INTERVAL_SEC;
        long windowMs = windowSec * 1000L;
        long excursionMs = excursionSec * 1000L;
        long queryIntervalMs = queryIntervalSec * 1000L;

        logger.info("QuerySync mode: start={}, window={}s, excursion={}s, interval={}s",
                fmtMs(startMs), windowSec, excursionSec, queryIntervalSec);

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
            PSpaceQueryExecutor executor = new PSpaceQueryExecutor(client, resolvedPoints, group);
            long totalRows = 0;

            // ============ Phase 1: Historical backfill ============
            long phase1End = System.currentTimeMillis();
            logger.info("Phase 1: historical backfill [{}, {})", fmtMs(startMs), fmtMs(phase1End));

            long windowStart = startMs;
            int windowIdx = 0;

            while (windowStart < phase1End) {
                long windowEnd = Math.min(windowStart + windowMs, phase1End);
                long queryStart = windowStart;

                windowIdx++;
                logger.info("Phase 1 window {}: query [{}, {})", windowIdx, fmtMs(queryStart), fmtMs(windowEnd));
                long rows = executor.queryRange(queryStart, windowEnd);
                logger.info("Phase 1 window {} completed: {} rows", windowIdx, rows);
                totalRows += rows;
                windowStart = windowEnd;
            }

            group.flushAll();
            logger.info("Phase 1 completed: {} total rows across {} windows", totalRows, windowIdx);

            // ============ Phase 2: Continuous sync ============
            long syncStart = phase1End;
            logger.info("Phase 2: continuous sync, interval={}s", queryIntervalSec);

            while (true) {
                Thread.sleep(queryIntervalMs);

                long syncEnd = System.currentTimeMillis();
                long queryStart = syncStart - excursionMs;

                logger.info("Phase 2: syncing [{}, {})", fmtMs(queryStart), fmtMs(syncEnd));

                long rows = executor.queryRange(queryStart, syncEnd);
                totalRows += rows;

                if (rows > 0) {
                    group.flushAll();
                    logger.info("Phase 2 sync: {} rows (total: {})", rows, totalRows);
                }

                syncStart = syncEnd;
            }
            // Phase 2 runs indefinitely; exit is triggered by taosx killing the process
        } finally {
            group.close();
            try {
                client.disconnect();
            } catch (Exception e) {
                /* ignore */ }
        }
    }
}
