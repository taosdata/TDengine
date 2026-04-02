import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class DataImporter {

    // Aliyun HiTSDB server instance address
    private static final String HITSDB_PUBLIC_ADDR = "ts-2zeg78wi9xk7837t3.hitsdb.rds.aliyuncs.com";
    private static final String HITSDB_VPC_ADDR = "ts-2zeg78wi9xk7837t3.hitsdb.tsdb.aliyuncs.com";
    // Aliyun HiTSDB server instance port number
    private static final int HITSDB_PUBLIC_PORT = 3242;
    private static final int HITSDB_VPC_PORT = 8242;
    // number of tags in simulated data
    private static final int TAG_NUM = 3;
    // number of metrics in simulated data
    private static final int METRIC_NUM = 5;
    // number of devices in simulated data, i.e. timelines
    private static final int DEVICE_NUM = 100;
    // number of data points to write in each batch
    private static int BATCH_SIZE = 1000;
    // number of batches needed for each insertion loop
    private static int BATCH_NUM;
    // number of threads used to write data
    private static final int THREAD_NUM = METRIC_NUM;
    // sleep time in milliseconds
    private static long SLEEP = 50;
    // system start time
    private static long systemStartTime =  System.currentTimeMillis();
    // starting time of generated data
    private static long dataStartTime =  1514736000000L; // 2018-01-01 00:00:00.000
    // start time offset
    private static long timeOffset = 0L;
    // tag2 values
    private static final String[] TAG3 = new String[] {
            "beijing",
            "shanghai",
            "hongkong",
    };
    // number of total successful insertions
    private static final AtomicLong succeeded = new AtomicLong(0);
//    private static long succeeded = 0L;
//     number of total failed insertions
    private static final AtomicLong failed = new AtomicLong(0);
//    private static long failed = 0L;
    // HiTSDB client
    private static HiTSDB hiTSDB;
    // data model
    private static ArrayList<PointMeta> pointMetas;

    class PointMeta {
        private String metric = null;
        private Map<String, String> tags = null;

        public String getMetric() {
            return metric;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {

        if (args.length < 4) {
            System.out.println("Invalid args! Please enter sleep time, batch size, time offset and network option!");
            return;
        } else {
            SLEEP = Long.valueOf(args[0]);
            BATCH_SIZE = Integer.valueOf(args[1]);
            timeOffset = Long.valueOf(args[2]);
        }

        BATCH_NUM = DEVICE_NUM * METRIC_NUM / BATCH_SIZE;
        // 为 HiTSDBConfig 配置参数，并创建HiTSDBConfig实例。
        // example.hitsdb.com 表示域名或地址。8242 表示 HiTSBD 的网络端口。您实际的域名地址和网络端口可到控制台获取。
        String HITSDB_ADDR =  "";
        int HITSDB_PORT = 0;
        if ("vpc".equalsIgnoreCase(args[3])) {
            System.out.println("Using VPC network connection...");
            HITSDB_ADDR = HITSDB_VPC_ADDR;
            HITSDB_PORT = HITSDB_VPC_PORT;
        } else {
            System.out.println("Using public network connection...");
            HITSDB_ADDR = HITSDB_PUBLIC_ADDR;
            HITSDB_PORT = HITSDB_PUBLIC_PORT;
        }
        HiTSDBConfig config = HiTSDBConfig.address(HITSDB_ADDR, HITSDB_PORT).config();
        // 通过 HiTSDBClientFactory 生成一个 HiTSDB 对象。
        hiTSDB = HiTSDBClientFactory.connect(config);

        System.out.println("=======================Parameters=======================");
        System.out.println("HITSDB_ADDR:" + HITSDB_ADDR);
        System.out.println("HITSDB_PORT:" + HITSDB_PORT);
        System.out.println("TAG_NUM:" + TAG_NUM);
        System.out.println("METRIC_NUM:" + METRIC_NUM);
        System.out.println("DEVICE_NUM:" + DEVICE_NUM);
        System.out.println("BATCH_SIZE:" + BATCH_SIZE);
        System.out.println("BATCH_NUM:" + BATCH_NUM);
        System.out.println("SLEEP:" + SLEEP);
        System.out.println("THREAD_NUM:" + THREAD_NUM);
        System.out.println("=========================================================");

        DataImporter dataImporter = new DataImporter();
        pointMetas = new ArrayList<>(DEVICE_NUM * METRIC_NUM);
        dataImporter.createMetaData();
        dataImporter.concurrentInsert();
//        dataImporter.insert();
        System.out.println("Application run is terminated. Closing HiTSDB client...");
        hiTSDB.close();
        System.exit(0);
    }

    private void createMetaData() {
        // create data model
        for (int d = 0; d < DEVICE_NUM; ++d) {
            // loop through all the metrics (columns in TDengine), similar to creating a row in TDengine
            for (int m = 0; m < METRIC_NUM; m++) {
                PointMeta pointMeta = new PointMeta();
                pointMeta.setMetric("tes." + m);
                Map<String, String> tags = new HashMap<>(TAG_NUM);
                tags.put("dId", String.valueOf(d));
                tags.put("gId", String.valueOf(d % THREAD_NUM));
                tags.put("l", TAG3[d % TAG_NUM]);
                pointMeta.setTags(tags);
                pointMetas.add(pointMeta);
            }
        }
    }

    private void insert() {
        long loop = 0;
        List<Point> pointBatch = new ArrayList<>(BATCH_SIZE);
        Long ts0 = dataStartTime;
        Long ts = ts0;
//        String logName = "m100d";
//        File file = new File("E:\\hitsdb\\log\\" + logName);
//        try {
//            FileWriter fileWriter = new FileWriter(file, true);
        SummaryResult result;
        try {
            while (loop < 1000000000) {
                // generate data for all devices (time lines)
                ts = ts0 + 1000 * (loop + timeOffset + 1);
                for (int i = 0; i < pointMetas.size(); i++) {
                    Point point = new Point();
                    point.setMetric(pointMetas.get(i).getMetric());
                    point.setTags(pointMetas.get(i).getTags());
                    point.setTimestamp(ts);
                    point.setValue(loop + timeOffset + 1);
                    pointBatch.add(point);
//                        fileWriter.write(point.toString() + "\n");
                    if (pointBatch.size() == BATCH_SIZE) {
                        result = hiTSDB.putSync(pointBatch, SummaryResult.class);
                        succeeded.addAndGet(result.getSuccess());
                        failed.addAndGet(result.getFailed());
//                        succeeded += result.getSuccess();
//                        failed += result.getFailed();
//                        if (result.getFailed() > 0) {
//                            System.out.println(result.toString());
//                        }
                        pointBatch.clear();
                        Thread.sleep(SLEEP);
                        System.out.printf("%s, total succeeded: %d, total failed: %d, time used: %ds, loop: %d\n",
                                new Date(System.currentTimeMillis()), succeeded, failed,
                                (System.currentTimeMillis() - systemStartTime) / 1000L, loop);
                    }
                }
                ++loop;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        }catch (Exception e1) {
//            e1.printStackTrace();
//        }
    }

    private void concurrentInsert() {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        for (int i = 0; i < THREAD_NUM; i ++) {
            executorService.execute(new Writer(i));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // wait for all threads to be terminated
        }
    }

    class Writer implements Runnable{
        private int threadId = 0;
        Writer(int threadId) {
            this.threadId = threadId;
        }
        public void run() {
            write();
        }
        public void write() {
            long loop = 0L;
            List<Point> pointBatch = new ArrayList<>(BATCH_SIZE);
            Long ts0 = dataStartTime + timeOffset;
            Long ts = ts0;
//            String logName = "m" + threadId;
//            File file = new File("E:\\hitsdb\\log\\" + logName);
//            try {
//                FileWriter fileWriter = new FileWriter(file, true);
                while (true) {
                    try {
                        // generate data for all devices (time lines)
                        ts = ts0 + 1000 * loop;
                        for (int i = threadId; i < pointMetas.size(); i += THREAD_NUM) {
                            Point point = new Point();
                            point.setMetric(pointMetas.get(i).getMetric());
                            point.setTags(pointMetas.get(i).getTags());
                            point.setTimestamp(ts);
                            point.setValue(loop);
                            pointBatch.add(point);
//                            fileWriter.write(point.toString() + "\n");
                            if (pointBatch.size() == BATCH_SIZE) {
                                SummaryResult result = hiTSDB.putSync(pointBatch, SummaryResult.class);
                                succeeded.addAndGet(result.getSuccess());
                                failed.addAndGet(result.getFailed());
                                if (result.getFailed() > 0) {
                                    System.out.println(result.toString());
                                }
                                pointBatch.clear();
                                Thread.sleep(SLEEP);
                            }
                        }
                        ++loop;
                        System.out.printf("%s, total succeeded: %d, total failed: %d, time used: %ds, loop: %d, threadId: %d\n",
                                new Date(System.currentTimeMillis()), succeeded.get(), failed.get(),
                                (System.currentTimeMillis() - systemStartTime) / 1000L, loop, threadId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
//            catch (IOException ioe) {
//                ioe.printStackTrace();
//            }
//            }
    }
}
