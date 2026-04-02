import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class DataRetriever {
    // Aliyun HiTSDB server instance address
    private static final String HITSDB_VPC_ADDR;
    private static final String HITSDB_PUBLIC_ADDR;
    // Aliyun HiTSDB server instance port number
    private static final int HITSDB_VPC_PORT;
    private static final int HITSDB_PUBLIC_PORT;
    // tag2 values
    private static final String[] TAG3 = new String[] {
            "beijing",
            "shanghai",
            "hongkong",
    };
    // number of total successful insertions
    private static final AtomicLong succeeded = new AtomicLong(0);
    // number of total failed insertions
    private static final AtomicLong failed = new AtomicLong(0);
    // HiTSDB client
    private static HiTSDB hiTSDB;
    // HiTSDB client config
    private static HiTSDBConfig config;
    // data model
    private static ArrayList<Point> points;

    // time
    private static double avgQueryTime = 0;

    static {
        HITSDB_VPC_ADDR = "ts-2zeg78wi9xk7837t3.hitsdb.tsdb.aliyuncs.com";
        HITSDB_PUBLIC_ADDR = "ts-2zeg78wi9xk7837t3.hitsdb.rds.aliyuncs.com";
        HITSDB_VPC_PORT = 8242;
        HITSDB_PUBLIC_PORT = 3242;
    }
    private static int repeat = 1;

    public static void main(String[] args) throws IOException {

        // read parameters
        if (args.length < 4) {
            // tss = args[0]
            // hours = args[1]
            // repeat = args[2]
            System.out.println("invalid args");
            return;
        }

        // 创建 HiTSDB 对象
        if ("vpc".equalsIgnoreCase(args[3].trim())) {
            System.out.println("Using VPC network connection...");
            config = HiTSDBConfig.address(HITSDB_VPC_ADDR, HITSDB_VPC_PORT).config();
        } else {
            System.out.println("Using public network connection...");
            config = HiTSDBConfig.address(HITSDB_PUBLIC_ADDR, HITSDB_PUBLIC_PORT).config();
        }

        hiTSDB = HiTSDBClientFactory.connect(config);
        DataRetriever dataRetriever = new DataRetriever();

        repeat = Integer.valueOf(args[2]);
        dataRetriever.query1(Long.valueOf(args[0]), Integer.valueOf(args[1]));
//        dataRetriever.query2(Long.valueOf(args[0]));
        // 安全关闭客户端，以防数据丢失。
        hiTSDB.close();
    }

    private void query1(long tss, int numOfDP) {
        // 构造查询条件并查询数据。
//        long tss = 1514736000000L; // 2018-01-01 00:00:00.000
        long tse = tss + numOfDP * 1000 - 1;
        // 查询一小时的数据
        String metric = "measure.0";
        Aggregator aggregator = Aggregator.NONE;
        Map<String, String> tags = new HashMap<>();
//        tags.put("devId", "0");
//        tags.put("grpId", "0");
//        tags.put("loc", "beijing");
        double totalPoints = numOfDP;

        Query query = Query.timeRange(tss, tse)
                .sub(SubQuery.metric(metric)
                        .aggregator(aggregator)
                        .tag(tags)
                        .build()).build();
        // 查询数据
        long ts0 = System.currentTimeMillis();
        List<QueryResult> result = hiTSDB.query(query);
        long timeUsed = System.currentTimeMillis() - ts0;
        System.out.printf("Time used for one query: %fs\n", BigDecimal.valueOf(timeUsed).divide(BigDecimal.valueOf(1000L)));
        // 打印输出
        if (result.size() > 0) {
            System.out.printf("Query on metric=%s\n\ttags:%s\n\ttime range: %s - %s\n\tdata points returned:%d\n",
                    metric, tags.toString(), new Date(tss), new Date(tse), result.get(0).getDps().size());
//        System.out.println(result);
            System.out.printf("Repeat %d times\n", repeat);
            for (int i = 0; i < repeat; i++) {
                result.clear();
                ts0 = System.nanoTime();
                result = hiTSDB.query(query);
                timeUsed = System.nanoTime() - ts0;
                avgQueryTime += timeUsed;
                System.out.println(timeUsed);
            }
            avgQueryTime = avgQueryTime / 1000000000 / repeat;
            BigDecimal timePerQuery = new BigDecimal(avgQueryTime);
            BigDecimal speedOfReading = new BigDecimal(totalPoints / avgQueryTime);
            System.out.printf("Average time used: %f s\n", timePerQuery);
            System.out.printf("Speed of reading: %f DP/s\n", speedOfReading);
        } else {
            System.out.println(result);
        }
    }

    private void query2(long tss) {
        // 构造查询条件并查询数据。
        long ts0 = System.currentTimeMillis();
//        long tss = 1545472800000L;
        // 查询一小时的数据
        String metric = "measurement0";
        Aggregator aggregator = Aggregator.COUNT;
        Map<String, String> tags = new HashMap<>();
        tags.put("deviceId", "0");
        tags.put("groupId", "0");
        tags.put("city", "beijing");

        Query query = Query.timeRange(tss, ts0)
                .sub(SubQuery.metric(metric)
                        .aggregator(aggregator)
                        .tag(tags)
                        .build()).build();
        // 查询数据
        List<QueryResult> result = hiTSDB.query(query);
        long timeUsed = System.currentTimeMillis() - ts0;
        System.out.printf("Time used for query: %fs\n", BigDecimal.valueOf(timeUsed).divide(BigDecimal.valueOf(1000L)));
        // 打印输出
        System.out.printf("Query on metric=%s\n\ttags:%s\n\ttime range: %s - %s\n\tdata points returned:%d\n",
                metric, tags.toString(), new Date(tss), new Date(ts0), result.get(0).getDps().size());
        System.out.println(result);
    }
}

