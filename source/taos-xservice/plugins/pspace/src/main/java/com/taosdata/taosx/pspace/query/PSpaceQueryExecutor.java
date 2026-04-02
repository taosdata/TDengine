package com.taosdata.taosx.pspace.query;

import com.sunwayland.pspace.PSpaceClient;
import com.sunwayland.pspace.entity.PsData;
import com.sunwayland.pspace.entity.PsHisData;
import com.sunwayland.pspace.entity.PsResult;
import com.sunwayland.pspace.enums.PsHisAggregateEnum;
import com.taosdata.taosx.pspace.PointResolver.ResolvedPoints;
import com.taosdata.taosx.pspace.ipc.DataTypeGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Historical data query executor implementing the "hisReadRawAll" algorithm.
 * <p>
 * The algorithm overcomes the 10,000-row-per-point limit of
 * {@code hisReadRawAsync} by:
 * <ol>
 * <li>Probing data distribution with {@code hisReadProcessed} (COUNT).</li>
 * <li>Greedy-merging adjacent intervals so no point exceeds 10,000 rows.</li>
 * <li>Async-querying each merged segment.</li>
 * <li>Cursor-based follow-up for truncated segments.</li>
 * </ol>
 *
 * @see <a href="../../../docs/dev/design/pspace-query.md">pspace-query.md</a>
 */
public class PSpaceQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(PSpaceQueryExecutor.class);

    /** Maximum rows per point per single hisReadRaw call. */
    private static final int MAX_BATCH = 10000;

    /** Number of sub-intervals for the probing step. */
    private static final int PROBE_INTERVALS = 100;

    /** Delay between consecutive async requests (ms). */
    private static final long ASYNC_DELAY_MS = 50;

    private final PSpaceClient client;
    private final ResolvedPoints points;
    private final DataTypeGroup group;

    /** Cumulative rows written to IPC. */
    private final AtomicLong totalRows = new AtomicLong(0);

    public PSpaceQueryExecutor(PSpaceClient client, ResolvedPoints points, DataTypeGroup group) {
        this.client = client;
        this.points = points;
        this.group = group;
    }

    /**
     * Query all raw historical data in {@code [startMs, endMs)} and send to
     * taosx via the DataTypeGroup writers.
     *
     * @param startMs inclusive start (epoch ms)
     * @param endMs   exclusive end (epoch ms)
     * @return total number of rows written
     */
    public long queryRange(long startMs, long endMs) throws Exception {
        List<Long> tagIds = points.getTagIds();
        if (tagIds.isEmpty()) {
            logger.warn("No tag IDs to query");
            return 0;
        }

        logger.debug("queryRange [{}, {}), {} points", startMs, endMs, tagIds.size());
        long rowsBefore = totalRows.get();

        // --- Step 1: Probe data distribution ---
        long[][] counts = probeDataDistribution(startMs, endMs, tagIds);

        // --- Step 2: Greedy-merge intervals into query segments ---
        List<long[]> segments = mergeSegments(startMs, endMs, counts, tagIds.size());
        logger.debug("Merged into {} query segments", segments.size());

        // --- Step 3 & 4: Query each segment ---
        for (int s = 0; s < segments.size(); s++) {
            long segStart = segments.get(s)[0];
            long segEnd = segments.get(s)[1];
            logger.debug("Querying segment {}/{}: [{}, {})", s + 1, segments.size(), segStart, segEnd);
            querySegment(segStart, segEnd, tagIds);
        }

        group.flushAll();
        long rowsInRange = totalRows.get() - rowsBefore;
        logger.debug("queryRange completed: {} rows", rowsInRange);
        return rowsInRange;
    }

    /**
     * Query a single time window (used by QueryTask for each window).
     * This is a simpler path when we trust the window is small enough.
     */
    public long queryWindow(long startMs, long endMs) throws Exception {
        List<Long> tagIds = points.getTagIds();
        if (tagIds.isEmpty())
            return 0;

        long rowsBefore = totalRows.get();
        long requestMs = System.currentTimeMillis();
        querySegmentDirect(startMs, endMs, tagIds, requestMs);
        group.flushAll();
        return totalRows.get() - rowsBefore;
    }

    // ===================== STEP 1: PROBE =====================

    /**
     * Use hisReadProcessed with PS_HIS_COUNT to probe how many data points
     * each tag has in each sub-interval.
     *
     * @return counts[interval][tagIndex] — estimated count per sub-interval per tag
     */
    private long[][] probeDataDistribution(long startMs, long endMs, List<Long> tagIds) {
        int n = Math.min(PROBE_INTERVALS, (int) Math.max(1, (endMs - startMs) / 1000));
        long intervalMs = (endMs - startMs) / n;
        if (intervalMs <= 0)
            intervalMs = endMs - startMs;
        long[][] counts = new long[n][tagIds.size()];

        try {
            List<PsHisAggregateEnum> aggregates = new ArrayList<>();
            for (int i = 0; i < tagIds.size(); i++) {
                aggregates.add(PsHisAggregateEnum.PS_HIS_COUNT);
            }

            PsResult<PsHisData> result = client.hisReadProcessed(
                    startMs, endMs, tagIds, aggregates, intervalMs);

            if (result.isSuccess() || result.isFailInBatch()) {
                // Parse the count results
                // Each PsHisData has a list of PsData per interval
                List<PsHisData> dataList = result.getData();
                if (dataList != null) {
                    for (int tagIdx = 0; tagIdx < dataList.size() && tagIdx < tagIds.size(); tagIdx++) {
                        PsHisData hd = dataList.get(tagIdx);
                        if (hd.getDataList() != null) {
                            for (int seg = 0; seg < hd.getDataList().size() && seg < n; seg++) {
                                PsData pd = hd.getDataList().get(seg);
                                if (pd.getValue() instanceof Number) {
                                    counts[seg][tagIdx] = ((Number) pd.getValue()).longValue();
                                }
                            }
                        }
                    }
                }
            } else {
                logger.warn("hisReadProcessed probe failed (code={}), using single segment fallback",
                        result.getCode());
                // Fallback: treat entire range as one segment
                for (int i = 0; i < n; i++) {
                    Arrays.fill(counts[i], 1); // Unknown but non-zero
                }
            }
        } catch (Exception e) {
            logger.warn("Probe failed, using single segment: {}", e.getMessage());
            for (int i = 0; i < n; i++) {
                Arrays.fill(counts[i], 1);
            }
        }

        return counts;
    }

    // ===================== STEP 2: MERGE =====================

    /**
     * Greedy-merge adjacent sub-intervals so that no tag's cumulative count
     * exceeds MAX_BATCH in a single merged segment.
     *
     * @return list of [segStartMs, segEndMs] pairs
     */
    private List<long[]> mergeSegments(long startMs, long endMs, long[][] counts, int numTags) {
        int n = counts.length;
        long intervalMs = (endMs - startMs) / n;
        if (intervalMs <= 0)
            intervalMs = endMs - startMs;

        List<long[]> segments = new ArrayList<>();
        int segStart = 0;
        long[] acc = new long[numTags]; // accumulated counts for current segment

        for (int i = 0; i < n; i++) {
            // Check if adding this interval would exceed MAX_BATCH for any tag
            boolean overflow = false;
            for (int t = 0; t < numTags; t++) {
                if (acc[t] + counts[i][t] > MAX_BATCH) {
                    overflow = true;
                    break;
                }
            }

            if (overflow && segStart < i) {
                // Emit the current segment (up to but not including i)
                long sMs = startMs + (long) segStart * intervalMs;
                long eMs = startMs + (long) i * intervalMs;
                segments.add(new long[] { sMs, eMs });
                segStart = i;
                Arrays.fill(acc, 0);
            }

            // Accumulate
            for (int t = 0; t < numTags; t++) {
                acc[t] += counts[i][t];
            }
        }

        // Final segment
        long sMs = startMs + (long) segStart * intervalMs;
        segments.add(new long[] { sMs, endMs });

        return segments;
    }

    // ===================== STEP 3 & 4: QUERY + BACKFILL =====================

    /**
     * Query a single segment asynchronously, then backfill truncated points.
     */
    private void querySegment(long segStart, long segEnd, List<Long> tagIds) throws Exception {
        long requestMs = System.currentTimeMillis();
        querySegmentDirect(segStart, segEnd, tagIds, requestMs);
    }

    /**
     * Direct query of a segment — async call + sync backfill for truncations.
     */
    private void querySegmentDirect(long segStart, long segEnd,
            List<Long> tagIds, long requestMs) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final List<PsHisData> resultHolder = new ArrayList<>();
        final Exception[] errorHolder = new Exception[1];

        Consumer<PsResult<PsHisData>> callback = result -> {
            try {
                if (result.isSuccess() || result.isFailInBatch()) {
                    if (result.getData() != null) {
                        resultHolder.addAll(result.getData());
                    }
                } else {
                    logger.error("hisReadRawAsync failed: code={}", result.getCode());
                    errorHolder[0] = new Exception("hisReadRawAsync failed: " + result.getCode());
                }
            } finally {
                latch.countDown();
            }
        };

        // Async query
        client.hisReadRawAsync(segStart, segEnd, tagIds, MAX_BATCH, false, callback);

        // Wait for callback
        if (!latch.await(300, TimeUnit.SECONDS)) {
            throw new Exception("hisReadRawAsync timed out after 300s");
        }
        if (errorHolder[0] != null) {
            throw errorHolder[0];
        }

        // Process results and detect truncation
        long receivedMs = System.currentTimeMillis();

        for (PsHisData hisData : resultHolder) {
            long tagId = hisData.getTagId();
            String name = points.getPointName(tagId);
            List<PsData> dataList = hisData.getDataList();
            if (dataList == null || dataList.isEmpty())
                continue;

            // Write data to IPC
            writeDataList(tagId, name, dataList, receivedMs, requestMs);

            // Step 4: Check for truncation and backfill
            if (dataList.size() >= MAX_BATCH) {
                long lastTs = dataList.get(dataList.size() - 1).getTimestamp();
                backfillTruncated(tagId, name, lastTs + 1, segEnd, requestMs);
            }
        }
    }

    /**
     * Step 4: Synchronous cursor-based backfill for a single truncated tag.
     */
    private void backfillTruncated(long tagId, String name, long fromMs, long endMs,
            long requestMs) throws Exception {
        logger.info("Backfilling truncated data for tagId={} from {}", tagId, fromMs);
        List<Long> singleTag = Collections.singletonList(tagId);

        while (fromMs < endMs) {
            PsResult<PsHisData> result = client.hisReadRaw(
                    fromMs, endMs, singleTag, MAX_BATCH, false);

            if (!result.isSuccess() && !result.isFailInBatch()) {
                logger.error("Backfill hisReadRaw failed for tagId={}: code={}",
                        tagId, result.getCode());
                break;
            }

            List<PsHisData> data = result.getData();
            if (data == null || data.isEmpty())
                break;

            PsHisData hisData = data.get(0);
            List<PsData> dataList = hisData.getDataList();
            if (dataList == null || dataList.isEmpty())
                break;

            long receivedMs = System.currentTimeMillis();
            writeDataList(tagId, name, dataList, receivedMs, requestMs);

            if (dataList.size() < MAX_BATCH) {
                break; // No more data
            }

            // Move cursor forward
            fromMs = dataList.get(dataList.size() - 1).getTimestamp() + 1;
        }
    }

    // ===================== WRITE =====================

    /**
     * Write a list of PsData records to the DataTypeGroup.
     */
    private void writeDataList(long tagId, String name, List<PsData> dataList,
            long receivedMs, long requestMs) throws IOException {
        for (PsData pd : dataList) {
            Object value = pd.getValue();
            long tsMs = pd.getTimestamp();
            int quality = pd.getQuality() != null ? pd.getQuality().ordinal() : 0;

            group.writeData(tagId, name, tsMs, receivedMs, value, quality, requestMs);
            totalRows.incrementAndGet();
        }
    }

    public long getTotalRows() {
        return totalRows.get();
    }
}
