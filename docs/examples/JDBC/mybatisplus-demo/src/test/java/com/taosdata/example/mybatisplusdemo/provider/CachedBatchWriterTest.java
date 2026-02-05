package com.taosdata.example.mybatisplusdemo.provider;

import com.taosdata.example.mybatisplusdemo.domain.Meters;
import com.taosdata.example.mybatisplusdemo.mapper.MetersMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test class for CachedBatchWriter.
 *
 * <p>Demonstrates PreparedStatement caching behavior.</p>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class CachedBatchWriterTest {

    private static final Random RANDOM = new Random();

    @Autowired
    private MetersMapper mapper;

    @Autowired
    private CachedBatchWriter writer;

    @Before
    public void setUp() {
        try { mapper.dropTable(); } catch (Exception ignored) {}
        mapper.createTable();
    }

    @After
    public void tearDown() {
        // Clear writer cache after each test
        writer.destroy();
    }

    /**
     * Test single batch insert.
     */
    @Test
    public void testSingleBatch() {
        List<Meters> data = createTestData(1000);
        writer.fastBatchWrite("com.taosdata.example.mybatisplusdemo.mapper.MetersMapper.insert", data);

        System.out.println("PreparedStatement created: " + writer.getPsCreateCount());
        // Expected: 1

        long count = mapper.selectCount(null);
        Assert.assertTrue("Should have inserted records", count >= data.size());
    }

    /**
     * Test multiple batches in a loop.
     *
     * <p>PreparedStatement created on first iteration, reused for subsequent iterations.</p>
     */
    @Test
    public void testMultipleBatches() {
        int batchSize = 100;
        int batchCount = 5;

        for (int i = 0; i < batchCount; i++) {
            List<Meters> data = createTestData(batchSize);
            writer.fastBatchWrite("com.taosdata.example.mybatisplusdemo.mapper.MetersMapper.insert", data);
        }

        System.out.println("PreparedStatement created: " + writer.getPsCreateCount());
        // Expected: 1 (created once, reused 4 times)

        long count = mapper.selectCount(null);
        Assert.assertTrue("Should have inserted records", count > 0);
    }

    /**
     * Test concurrent batch writes with multiple threads.
     *
     * <p>Demonstrates multi-threaded scenario where each thread maintains its own
     * PreparedStatement cache.</p>
     */
    @Test
    public void testConcurrentBatches() throws InterruptedException {
        int threadCount = 5;
        int batchesPerThread = 3;
        int batchSize = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        // Reset counter before test
        long initialCount = writer.getPsCreateCount();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < batchesPerThread; j++) {
                        List<Meters> data = createTestData(batchSize, threadId, j);
                        writer.fastBatchWrite(
                            "com.taosdata.example.mybatisplusdemo.mapper.MetersMapper.insert",
                            data
                        );
                    }
                    successCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);  // Wait for all threads to finish

        System.out.println("PreparedStatement created: " +
            (writer.getPsCreateCount() - initialCount) +
            " (expected: " + threadCount + ", one per thread)");
        System.out.println("Successful threads: " + successCount.get() + "/" + threadCount);

        long totalCount = mapper.selectCount(null);
        long expectedCount = (long) threadCount * batchesPerThread * batchSize;
        Assert.assertTrue("Should have inserted records", totalCount >= expectedCount - 100);
        Assert.assertEquals("All threads should succeed", threadCount, successCount.get());
    }

    private List<Meters> createTestData(int count) {
        return createTestData(count, 0, 0);
    }

    private List<Meters> createTestData(int count, int threadId, int batchId) {
        List<Meters> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Meters m = new Meters();
            m.setTbname("device_t" + threadId + "_b" + batchId + "_" + i);
            m.setTs(new Timestamp(System.currentTimeMillis() + i));
            m.setCurrent(RANDOM.nextFloat());
            m.setVoltage(220);
            m.setPhase(RANDOM.nextFloat());
            m.setGroupid(threadId * 1000 + batchId * 100 + i);
            data.add(m);
        }
        return data;
    }
}
