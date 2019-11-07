package com.taosdata.iot.javaTestSuit.tools.taosRestClient;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.taosdata.iot.javaTestSuit.Exceptions.TaosHttpException;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaosRestWriter {
    // connection params
    private static String host = "127.0.0.1";
    private static String user = "root";
    private static String password = "taosdata";
    // Authorization
    private static String auth = "Basic " + Base64.getEncoder().encodeToString("root:taosdata".getBytes());

    // schema params
    private static String[] cols = new String[]{"ts timestamp", "c1 int"}; // column defination
    private static String[] tags = new String[]{"t1 int"}; // tag defination
    private static String db = "db"; // database name
    private static String mt = "mt"; // metric name
    private static String tbPrefix = "tb"; // table name

    // data scale
    private static long timestep = 1; // sample time interval in milliseconds
    private static long tbNum = 10; // number of tables
    private static long dataStartTime; // historical data start time
    private static long dataTimeRange = 365; // time range for historical data (in days)
    private static long totalRowsPerTable = 0;

    // retention params
    private static int replica = 1;
    private static int days = 10;
    private static int keep = 3650;
    private static int tables = 1000;
    private static int rows = 4096;
    private static int cache = 16384;
    private static double ablocks = 4;
    private static int tblocks = 512;
    private static int ctime = 3600;
    private static int clog = 0;
    private static int comp = 2;
    private static String precision = "ms";

    // insert option params
    private static String insertOption;
    private static int threadNum = 1;
    private static int batchSize = 10;
    private static int batchNum = 100;
    private static long sleepBetweenInserts = 0L;

    // Constants
    private static final String STATUS_ERROR = "error";
    private static final String STATUS_SUCC = "succ";

    // server ip address
    private static String ENDPOINT = "http://localhost:6020/rest/sql";

    private final static Logger logger = LoggerFactory.getLogger(TaosRestWriter.class);
    private final static Gson gson = new Gson();

    public static void main(String[] args) throws IOException {

        TaosRestWriter taosRestWriter = new TaosRestWriter();
        if (args != null && args[0] != null) {
            taosRestWriter.config(args[0]);
        } else {
            logger.error("Failed to read configuration file, file path is NULL");
        }
        taosRestWriter.setupSchema();
        taosRestWriter.concurrentImport();
        return;

    }

    private void config(String jsonFilePath) {
//        System.out.println("Reading configuration parameters from the json file...");
        logger.info("Reading configuration parameters from the json file...");
        JsonParser jsonParser = new JsonParser();
        Gson gson = new Gson();
        try {
            JsonObject params = (JsonObject) jsonParser.parse(new FileReader(jsonFilePath));
            if (!Strings.isNullOrEmpty(params.get("host").getAsString())) {
                host = params.get("host").getAsString();
            }
            if (!Strings.isNullOrEmpty(params.get("user").getAsString())) {
                user = params.get("user").getAsString();
            }
            if (!Strings.isNullOrEmpty(params.get("password").getAsString())) {
                password = params.get("password").getAsString();
            }

            db = params.get("db").getAsString();
            mt = params.get("mt").getAsString();
            tbPrefix = params.get("tbPrefix").getAsString();
            cols = params.get("cols").getAsString().split(",");
            tags = params.get("tags").getAsString().split(",");
            timestep = params.get("timestep").getAsLong();
            tbNum = params.get("tbNum").getAsLong();
            dataStartTime = params.get("dataStartTime").getAsLong();
            dataTimeRange = params.get("dataTimeRange").getAsLong();

            replica = params.get("replica").getAsInt();
            days = params.get("days").getAsInt();
            keep = params.get("keep").getAsInt();
            tables = params.get("tables").getAsInt();
            rows = params.get("rows").getAsInt();
            cache = params.get("cache").getAsInt();
            ablocks = params.get("ablocks").getAsDouble();
            tblocks = params.get("tblocks").getAsInt();
            ctime = params.get("ctime").getAsInt();
            clog = params.get("clog").getAsInt();
            comp = params.get("comp").getAsInt();
            precision = params.get("precision").getAsString();

//            insertOption = params.get("insertOption").getAsString();
            threadNum = params.get("threadNum").getAsInt();
//            batchSize = params.get("batchSize").getAsInt();
//            batchNum = params.get("batchNum").getAsInt();
            sleepBetweenInserts = params.get("sleepBetweenInserts").getAsLong();
            if (!"root".equals(user) && !"taosdata".equals(password)) {
                auth = "Basic " + Base64.getEncoder().encodeToString((user+ ":" + password).getBytes());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (dataStartTime <= 0) {
            dataStartTime = System.currentTimeMillis();
        }
        totalRowsPerTable = (long)(dataTimeRange * 24 * 3600 * 1000 / timestep);
        batchNum = (int)totalRowsPerTable / batchSize;
        ENDPOINT = "http://" + host + ":6020/rest/sql";

        System.out.println("Parameters are set!");

        System.out.println("=======================Paramerters========================");
        System.out.printf("Host IP: \t%s\n", host);
        System.out.printf("User: \t%s\n", user);
        System.out.printf("Password: \t%s\n", password);
        System.out.printf("Database name: \t%s\n", db);
        System.out.printf("STable name: \t%s\n", mt);
        System.out.printf("Table prefix: \t%s\n", tbPrefix);
        System.out.printf("Columns: \t%s\n", Arrays.asList(cols).toString());
        System.out.printf("Tags: \t%s\n", Arrays.asList(tags).toString());
        System.out.printf("Time step: \t%d ms\n", timestep);
        System.out.printf("Number of tables: \t%d\n", tbNum);
        System.out.printf("Time range: \t%d days\n", dataTimeRange);
        System.out.printf("Data start time: \t%s\n", new Timestamp(dataStartTime));
        System.out.printf("Data end time: \t%s\n", new Timestamp(dataStartTime + totalRowsPerTable * timestep));
        System.out.printf("Approximate number of rows expected in each table: \t%d\n", batchNum * batchSize);
        System.out.printf("Sleep between insertions: \t%d ms\n", sleepBetweenInserts);
//        System.out.printf("replica: %d\n", replica);
//        System.out.printf("days: %d\n", days);
//        System.out.printf("keep: %d\n", keep);
//        System.out.printf("tables: %d\n", tables);
//        System.out.printf("rows: %d\n", rows);
//        System.out.printf("cache: %d\n", cache);
//        System.out.printf("ablocks: %f\n", ablocks);
//        System.out.printf("tblocks: %d\n", tblocks);
//        System.out.printf("ctime: %d\n", ctime);
//        System.out.printf("clog: %d\n", clog);
//        System.out.printf("comp: %d\n", comp);
//        System.out.printf("precision: %s\n", precision);
        System.out.printf("Number of threads to use: \t%d\n", threadNum);
        System.out.printf("Percentage of imports: \t%d%%\n", 30);
//        System.out.printf("batch size for insertion: \t%d\n", batchSize);
//        System.out.printf("number of batches needed: \t%d\n", batchNum);
        System.out.println("==========================================================");
    }

    public void setupSchema() {
        String sql = "drop database if exists " + db;
        try (CloseableHttpClient httpClient = HttpClients.createMinimal()){
            logger.debug("Executing SQL: {}", sql);
            TaosHttpResponseBody httpResponseBody;
            httpResponseBody = executeHttpQuery(sql, httpClient);
            if (STATUS_ERROR.equals(httpResponseBody.getStatus())) {
                logger.error("Failed to execute SQL: {} \nError message: '{}'", sql, httpResponseBody.getDesc());
                System.exit(0);
            }

            sql = "create database " + db;
            logger.debug("Executing SQL: {}", sql);
            httpResponseBody = executeHttpQuery(sql, httpClient);
            if (STATUS_ERROR.equals(httpResponseBody.getStatus())) {
                logger.error("Failed to execute SQL: {} \nError message: '{}'", sql, httpResponseBody.getDesc());
                System.exit(0);
            }

            sql = SqlGenerator.getCreateMetricSql(db + "." + mt, cols, tags);
            httpResponseBody = executeHttpQuery(sql, httpClient);
            if (STATUS_ERROR.equals(httpResponseBody.getStatus())) {
                logger.error("Failed to execute SQL: {} \nError message: '{}'", sql, httpResponseBody.getDesc());
                System.exit(0);
            }

            for (int i = 1; i <= tbNum; i++) {
                String[] tagValues = new String[tags.length];
                for (int j = 0; j < tagValues.length; j++) {
                    tagValues[j] = String.valueOf((j+1)%i);
                }
                sql = SqlGenerator.getCreateTableUsingMetricSql(db + "." + tbPrefix + i, db + "." + mt, tagValues);
                httpResponseBody = executeHttpQuery(sql, httpClient);
                if (STATUS_ERROR.equals(httpResponseBody.getStatus())) {
                    logger.error("Failed to execute SQL: {} \nError message: '{}'", sql, httpResponseBody.getDesc());
                    System.exit(0);
                }
            }
            logger.info("Schema has been set up!");
        } catch (Exception e) {

        }
    }

    public void concurrentImport() {
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
//            executorService.execute(new TaosWritter(i));
            new Thread(new TaosWritter(i)).run();
        }
//        executorService.shutdown();
//        try {
//            executorService.awaitTermination(100000, TimeUnit.MILLISECONDS);
//        } catch (InterruptedException ie) {
//            ie.printStackTrace();
//            logger.error("ExecutorService quited with an interrupted exception!");
//            System.exit(0);
//        }
//        try {
//            while (!executorService.isTerminated()) {
//                // wait
//                Thread.currentThread().sleep(3000);
//            }
//        } catch (Exception e) {
//
//            e.printStackTrace();
//        }
//        logger.info("Concurrent write completed!");
    }

    private class TaosWritter implements Runnable {
        private int threadId;

        public TaosWritter(int i) {
            threadId = i;
        }
        @Override
        public void run() {
            long t1 = 1547700000000L; // 2019-01-17 13:51:20.000
            Random random = new Random();
            long ts = dataStartTime;
            int tbId = 0;
            String jsonQuery;
            try (CloseableHttpClient httpClient = HttpClients.createMinimal()) {
                for (int i = 1; i <= totalRowsPerTable; i++) {
                    logger.debug("Loop {}", i);
                    tbId = random.nextInt((int)tbNum) + 1;
                    ts = timestep * (random.nextInt(10) - 3) + ts;
//                    jsonQuery = "insert into db_gps_info.v" + tbId + "_alarm values (" + ts + ", '2019-01-17 09:35:41.000', " + i % 10000 + ", 109.203745, 30.120334, 67.0)";
                    jsonQuery = "insert into " + db + "." + tbPrefix + tbId + " values (" + ts + ", 120.684605000, 32.068486000, 13, 23, 0, 109, 28755.100, 0, " + i + ", " + threadId + ", 0, NULL, NULL, 25, NULL, true, now, true)";
                    TaosHttpResponseBody httpResponseBody;
                    httpResponseBody = executeHttpQuery(jsonQuery, httpClient);
                    logger.debug("Executing: {}", jsonQuery);
                    if (STATUS_SUCC.equals(httpResponseBody.getStatus())) {
                        if ("0".equals(httpResponseBody.getData().get(0).get(0))) {
                            logger.debug("0 row affected by insert, retry with import");
                            jsonQuery = jsonQuery.replaceFirst("insert", "import");
                            logger.debug("{}", jsonQuery);
                            httpResponseBody = executeHttpQuery(jsonQuery, httpClient);
                            if (STATUS_SUCC.equals(httpResponseBody.getStatus())) {
                                if ("0".equals(httpResponseBody.getData().get(0).get(0))) {
                                    logger.debug("0 row affected by import, timestamp has already existed");
                                } else {
                                    logger.debug("1 row imported");
                                }
                            } else {
                                logger.error("Failed to import data \nError message: {}", httpResponseBody.getDesc());
                            }
                        } else {
                            logger.debug("1 row inserted");
                        }
                    } else {
                        logger.error("Failed to execute SQL: {} \nError message: {}", jsonQuery, httpResponseBody.getDesc());
                    }

//                    fileWriter.write(jsonQuery + ";\n");
                    Thread.currentThread().sleep(sleepBetweenInserts);
                    jsonQuery = "select * from " + db + "." + tbPrefix + tbId + " limit 100";
                    httpResponseBody = executeHttpQuery(jsonQuery, httpClient);
                    if (STATUS_ERROR.equals(httpResponseBody.getStatus())) {
                        logger.error("Failed to execute SQL: {} \nError message: {}", jsonQuery, httpResponseBody.getDesc());
                    }
//                    fileWriter.write(jsonQuery + ";\n");
                    logger.debug("Loop {}", i);
                }
//                fileWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return;
        }
    }


    public TaosHttpResponseBody executeHttpQuery(String queryJsonStr, HttpClient httpClient) throws Exception {
        double avgTime = 0;
        TaosHttpResponseBody httpResponseBody;
        try {
//            CloseableHttpClient httpClient = HttpClients.createDefault();
//            httpClient = HttpClients.createMinimal();
            HttpPost httpPost = new HttpPost(ENDPOINT);
            httpPost.addHeader("Authorization", auth);

            StringEntity queryJsonStrEntity = new StringEntity(queryJsonStr);
            queryJsonStrEntity.setContentType(ContentType.APPLICATION_JSON.toString());
            httpPost.setEntity(queryJsonStrEntity);

            long start;
            long timeUsed;
            HttpResponse response;
            start = System.currentTimeMillis();
            response = httpClient.execute(httpPost);
            httpResponseBody = gson.fromJson(EntityUtils.toString(response.getEntity()), TaosHttpResponseBody.class);
            timeUsed = (System.currentTimeMillis() - start);
            logger.info("Time used: {} ms", timeUsed);
            return httpResponseBody;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TaosHttpException();
        }
    }

    class TaosHttpResponseBody {
        String status;
        ArrayList<String> head;
        ArrayList<ArrayList<String>> data;
        Integer rows;
        Integer code;
        String desc;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public ArrayList<String> getHead() {
            return head;
        }

        public void setHead(ArrayList<String> head) {
            this.head = head;
        }

        public ArrayList<ArrayList<String>> getData() {
            return data;
        }

        public void setData(ArrayList<ArrayList<String>> data) {
            this.data = data;
        }

        public Integer getRows() {
            return rows;
        }

        public void setRows(Integer rows) {
            this.rows = rows;
        }

        public Integer getCode() {
            return code;
        }

        public void setCode(Integer code) {
            this.code = code;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }
    }
}
