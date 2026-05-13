package com.taosdata.taosx.pspace.run;

import com.sunwayland.pspace.PSpaceClient;
import com.sunwayland.pspace.entity.PsResult;
import com.sunwayland.pspace.entity.PsSubRealData;
import com.sunwayland.pspace.callback.IRealCallback;
import com.taosdata.taosx.pspace.PointResolver;
import com.taosdata.taosx.pspace.PointResolver.ResolvedPoints;
import com.taosdata.taosx.pspace.config.Configuration;
import com.taosdata.taosx.pspace.ipc.DataTypeGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements the "Subscribe" run mode: real-time data subscription.
 * <p>
 * Uses {@code client.realNewSubscribeAndRead(tagIds, callbackList)} to
 * subscribe to live data from pSpace and stream it to taosX via Arrow IPC.
 * <p>
 * Subscribe mode runs indefinitely until the process is terminated by taosx.
 */
public class SubscribeTask {

    private static final Logger logger = LoggerFactory.getLogger(SubscribeTask.class);

    public int execute(Configuration cfg) throws Exception {
        logger.info("Subscribe mode starting");

        // --- Connect to pSpace ---
        PSpaceClient client = cfg.tryConnect();

        // --- Resolve points ---
        ResolvedPoints resolvedPoints = PointResolver.resolve(cfg, client);
        logger.info("Resolved {} points for subscription", resolvedPoints.size());

        // --- Setup IPC ---
        DataTypeGroup group = DataTypeGroup.create(cfg, resolvedPoints);
        if (!group.isLocalOnly()) {
            group.connect();
        }

        AtomicLong totalRows = new AtomicLong(0);
        CountDownLatch keepAlive = new CountDownLatch(1);

        try {
            List<Long> tagIds = resolvedPoints.getTagIds();

            // --- Define callback ---
            IRealCallback callback = (int subId, List<PsSubRealData> subRealData) -> {
                try {
                    logger.info("Subscribe callback triggered: subId={}, dataSize={}",
                            subId, subRealData == null ? 0 : subRealData.size());
                    if (subRealData == null || subRealData.isEmpty())
                        return;

                    long now = System.currentTimeMillis();

                    for (PsSubRealData data : subRealData) {
                        long tagId = data.getTagId();
                        String name = resolvedPoints.getPointName(tagId);
                        long tsMs = data.getTimestamp();
                        Object value = data.getValue();
                        int quality = data.getQuality() != null ? data.getQuality().ordinal() : 0;

                        // For Subscribe, received == request (passive push, no separate request moment)
                        group.writeData(tagId, name, tsMs, now, value, quality, now);
                        totalRows.incrementAndGet();
                    }

                    // Flush after each callback to ensure data is written promptly
                    group.flushAll();
                } catch (Exception e) {
                    logger.error("Error in subscribe callback: {}", e.getMessage(), e);
                }
            };

            // --- Subscribe ---
            PsResult<PsSubRealData> result = client.realNewSubscribeAndRead(
                    tagIds, Collections.singletonList(callback));

            if (result.getCode() == null || !result.isSuccess()) {
                logger.error("Subscribe failed: code={}", result.getCode());
                return -1;
            }

            // --- Process initial values ---
            List<PsSubRealData> initialValues = result.getData();
            if (initialValues != null && !initialValues.isEmpty()) {
                long now = System.currentTimeMillis();
                logger.info("Processing {} initial values", initialValues.size());

                for (PsSubRealData data : initialValues) {
                    long tagId = data.getTagId();
                    String name = resolvedPoints.getPointName(tagId);
                    long tsMs = data.getTimestamp();
                    Object value = data.getValue();
                    int quality = data.getQuality() != null ? data.getQuality().ordinal() : 0;

                    group.writeData(tagId, name, tsMs, now, value, quality, now);
                    totalRows.incrementAndGet();
                }

                group.flushAll();
                logger.info("Initial values sent: {} rows", totalRows.get());
            }

            // --- Keep alive ---
            logger.info("Subscribe active, waiting for data pushes...");
            keepAlive.await(); // Blocks forever; taosx will kill the process to stop

            return 0;
        } catch (InterruptedException e) {
            logger.info("Subscribe interrupted");
            return 0;
        } finally {
            try {
                group.flushAll();
            } catch (Exception e) {
                /* ignore */ }
            group.close();
            try {
                client.disconnect();
            } catch (Exception e) {
                /* ignore */ }
            logger.info("Subscribe mode completed: {} total rows", totalRows.get());
        }
    }
}
