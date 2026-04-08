
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
public class PerfMain {
    // 模拟数据 Tag 个数
    private static final int TAG;
    // 每秒生成的数据点数
    private static final int SOURCE_RATE;
    // 批量写入点数
    private static final int BATCH;
    // 并发数
    private static final int THREADS;
    // 实例地址
    private static final String HITSDB_ADDR;
    // 实例端口
    private static final int HITSDB_PORT;
    public static int getEnvInt(String envName, int value) {
        String env = System.getenv(envName);
        if (env != null && !env.equals("")) {
            try {
                value = Integer.parseInt(env);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return value;
    }
    // 超时时间
    private static final int SYNC_TIMEOUT_MS;
    // 重试次数
    private static final int MAX_TRY;
    static {
        TAG = getEnvInt("TAG", 6);
        SOURCE_RATE = getEnvInt("SOURCE_RATE", 50000);
        BATCH = getEnvInt("BATCH", 400);
        THREADS = getEnvInt("THREADS", 256);
        HITSDB_PORT = getEnvInt("HITSDB_PORT", 8242);
        HITSDB_ADDR = System.getenv("HITSDB_ADDR");
        MAX_TRY = getEnvInt("MAX_TRY", 4);
        SYNC_TIMEOUT_MS = getEnvInt("SYNC_TIMEOUT_MS", 2 * 60 * 1000);
        System.out.println("TAG:" + TAG);
        System.out.println("SOURCE_RATE:" + SOURCE_RATE);
        System.out.println("BATCH:" + BATCH);
        System.out.println("THREADS:" + THREADS);
        System.out.println("HITSDB_ADDR:" + HITSDB_ADDR);
        System.out.println("HITSDB_PORT:" + HITSDB_PORT);
        System.out.println("SYNC_TIMEOUT_MS:" + SYNC_TIMEOUT_MS);
        System.out.println("MAX_TRY:" + MAX_TRY);
    }
    // 写入路径
    private final String putUrl = "http://" + HITSDB_ADDR + ":" + HITSDB_PORT + "/api/put?sync_timeout="
            + SYNC_TIMEOUT_MS;
    private final AtomicLong successed = new AtomicLong();
    private final DataSource dataSource = new DataSource();
    private final ArrayList<Thread> THREAD_POOL = new ArrayList<Thread>();
    // 数据点类型
    static class DataPoint {
        public String metric;
        public Long timestamp;
        public Double value;
        public Map<String, String> tags;
    }
    class DataSource {
        private final BlockingQueue<String> flowQueue = new ArrayBlockingQueue<String>(10000);
        private final BlockingQueue<String> dataQueue = new ArrayBlockingQueue<String>(10000);
        private final BlockingQueue<String> retryQueue = new ArrayBlockingQueue<String>(10000);
        private final Timer timer = new Timer(true);
        private final List<Thread> threads = new ArrayList<Thread>();
        public void start() {
            // 模拟数据发生器
            for (int i = 0; i < 4; ++i) {
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Long loop = 0L;
                        // 生成数据模型
                        ArrayList<DataPoint> builder = new ArrayList<DataPoint>();
                        for (int b = 0; b < BATCH; ++b) {
                            DataPoint dataPoint = new DataPoint();
                            dataPoint.timestamp = System.currentTimeMillis();
                            dataPoint.value = 3.14;
                            dataPoint.metric = "perf_metric_" + b;
                            dataPoint.tags = new HashMap<String, String>();
                            for (int t = 0; t < TAG; ++t) {
                                dataPoint.tags.put("tagk" + t, "tagv" + t);
                            }
                            builder.add(dataPoint);
                        }
                        while (true) {
                            try {
                                String data = null;
                                if (retryQueue.size() > 0) {
                                    data = retryQueue.poll(0, TimeUnit.MILLISECONDS);
                                }
                                if (data != null) {
                                    dataQueue.put(data);
                                } else {
                                    // 生成每条时间线上的数据
                                    for (int i = 0; i < BATCH; ++i) {
                                        DataPoint dataPoint = builder.get(i);
                                        dataPoint.timestamp += loop * 1000;
                                    }
                                    dataQueue.put(JSON.toJSONString(builder));
                                    ++loop;
                                }
                            } catch (Exception e) {
                            }
                        }
                    }
                });
                thread.setDaemon(true);
                thread.start();
                threads.add(thread);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
            }
            // 流控
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    for (int i = 0; i < SOURCE_RATE; i += BATCH) {
                        String elem = dataQueue.peek();
                        if (elem == null) {
                            System.out.println("pull null, data gen too low");
                            break;
                        } else {
                            if (flowQueue.offer(elem)) {
                                try {
                                    dataQueue.take();
                                } catch (InterruptedException e) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }, 0, 1000);
        }
        public String pull() throws InterruptedException {
            return flowQueue.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
        }
        public void retryPush(String data) throws InterruptedException {
            retryQueue.put(data);
            if(retryQueue.size() > 1000) {
                System.out.println("Retry queued " + retryQueue.size());
            }
        }
    }
    public void start() {
        dataSource.start();
        // 并发写
        for (int i = 0; i < THREADS; ++i) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            String data = dataSource.pull();
                            int try_count = 0;
                            for (; try_count < MAX_TRY; ++try_count) {
                                CloseableHttpClient httpClient = HttpClients.createDefault();
                                HttpPost httpPost = new HttpPost(putUrl);
                                StringEntity eStringEntity = new StringEntity(data, "utf-8");
                                eStringEntity.setContentType("application/json");
                                httpPost.setEntity(eStringEntity);
                                HttpResponse response = httpClient.execute(httpPost);
                                int statusCode = response.getStatusLine().getStatusCode();
                                if (statusCode != 200 && statusCode != 204) {
                                    httpClient.close();
                                    Thread.sleep(100);
                                } else {
                                    successed.addAndGet(BATCH);
                                    httpClient.close();
                                    break;
                                }
                            }
                            if (try_count == MAX_TRY) {
                                dataSource.retryPush(data);
                            }
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
            THREAD_POOL.add(thread);
        }
    }
    public void test() throws InterruptedException {
        // 统计 TPS 的时间间隔
        final Long showTimeMs = 10 * 1000L;
        final AtomicLong lastSuccess = new AtomicLong();
        start();
        Timer tps = new Timer(true);
        tps.schedule(new TimerTask() {
            @Override
            public void run() {
                Long nowSuccess = successed.get();
                System.out.println("total success: " + nowSuccess.longValue() + " tps: "
                        + (nowSuccess - lastSuccess.get()) * 1000 / showTimeMs);
                lastSuccess.set(nowSuccess);
            }
        }, showTimeMs, showTimeMs);
    }
    public static void main(String[] args) throws InterruptedException {
        PerfMain demo = new PerfMain();
        demo.test();
        // 测试执行时间
        final Long testRunSeconds = (long) getEnvInt("RUN_SECONDS", 5 * 60);
        System.out.println("RUN_SECONDS:" + testRunSeconds);
        Thread.sleep(testRunSeconds * 1000);
        System.exit(0);
    }
}