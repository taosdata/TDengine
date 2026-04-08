import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TdRestClientTester {
    // server ip address
//    private static final String ENDPOINT = "http://58.87.127.226:6020/rest/sql";
    private static final String ENDPOINT = "http://localhost:6020/rest/sql";

    private static int NUM_OF_TABLES = 500;

    private static int repeat = 1;
    private static boolean schemaAltered =  false;
    private volatile static boolean[] schemaUpdated = new boolean[NUM_OF_TABLES];

    public static void main(String[] args) throws IOException {

        System.out.println("Please enter test cases: ('import' or 'alter')");
        Scanner scanner = new Scanner(System.in);
        String test = scanner.nextLine();
        scanner.close();

        TdRestClientTester tdRestClientTester = new TdRestClientTester();

        if ("import".equalsIgnoreCase(test)) {
            System.out.println("'import' test is chosen!");
            tdRestClientTester.setupSchema();
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            tdRestClientTester.concurrentImport();

        } else if ("alter".equalsIgnoreCase(test)) {
            System.out.println("'alter' test is chosen!");
            tdRestClientTester.setupSchema();
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            tdRestClientTester.concurrentImportWithAlter();
        } else {
            System.out.printf("Invalid test case name: %s\n", test);
            System.out.println("Please enter 'import' or 'alter'!");
            System.out.println("========================================================================================================\n" +
                              "'import' case set up: concurrently importing into 500 tables under STable 'alarm' with a constraint that \n" +
                              " the primary timestamps of all records imported here are randomly picked from a certain timestamp pool\n" +
                              " which contains only 200 different values.\n\n" +
                              "'alter' case set up: concurrently importing into 500 tables under STable 'alarm'. The timestamps of a new \n" +
                              " record will be generally increasing but with chances that to decrease at occasional points. The main \n" +
                              " thread will at some point change the schema of 'alarm', then the importing threads should automatically \n" +
                              " change the import schema, and use an insert operation to update the schema in cache and then import with \n" +
                              " the new schema.\n" +
                              "========================================================================================================\n");
            return;
        }


    }


    public void setupSchema() {
        System.out.println("drop database if exists db_gps_info");
        String jsonQuery = "drop database if exists db_gps_info";
        executeQueryJson(jsonQuery);
        System.out.println("creating db and tables...");
        jsonQuery = "create database db_gps_info";
        executeQueryJson(jsonQuery);
        jsonQuery = "create table db_gps_info.alarm ( create_date TIMESTAMP, alarm_time TIMESTAMP,  type_id SMALLINT,  lngitude DOUBLE,  latitude DOUBLE,  velocity FLOAT) " +
                    "tags (vid BINARY(9), user_id BINARY(8))";
        executeQueryJson(jsonQuery);
        for (int i = 1; i <= NUM_OF_TABLES; i++) {
            jsonQuery = "create table db_gps_info.v" + i + "_alarm using db_gps_info.alarm tags('v" + i + "', 'user" + i +"')";
            executeQueryJson(jsonQuery);
        }
        System.out.println("schema has been set up!");
    }

    public void concurrentImport() {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            executorService.execute(new Importer(i));
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // wait
        }
        System.out.println("concurrentImport completed!");
    }

    public void concurrentImportWithAlter() {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            executorService.execute(new ImporterWithAlter(i));
        }
        try {
            System.out.printf("%s sleeps for 20s...", Thread.currentThread().getName());
            Thread.currentThread().sleep(20000);
            System.out.println("Alter schema now!");
            String jsonQuery = "alter table db_gps_info.alarm add column acc bool";
            System.out.printf("%s", jsonQuery);
            executeQueryJson(jsonQuery);

        } catch (Exception e) {
            e.printStackTrace();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // wait
        }
        System.out.println("concurrentImport completed!");
    }

    private class Importer implements Runnable {
        private int threadId;

        public Importer() {

        }

        public Importer(int i) {
            threadId = i;
        }
        @Override
        public void run() {
            long t1 = 1547700000000L; // 2019-01-17 13:51:20.000
            Random random = new Random();
            long ts = 0L;
            int tbid = 0;
            String jsonQuery;
            String logName = "importTestLogThread_" + threadId + ".sql";
            File file = new File("/home/" + logName);
//            File file = new File("E:\\hitsdb\\log\\" + logName);
            try {
                FileWriter fileWriter = new FileWriter(file, true);
                for (int i = 1; i <= 1000000; i++) {
//                tbid = random.nextInt(NUM_OF_TABLES) + 1;
                    tbid = random.nextInt(100) + threadId * 100 + 1;
                    ts = 1000 * (random.nextInt(10) - 4) + ts;
//                    jsonQuery = "insert into db_gps_info.v" + tbid + "_alarm values (" + ts + ", '2019-01-17 09:35:41.000', " + i % 10000 + ", 109.203745, 30.120334, 67.0)";
                    jsonQuery = "import into db_gps_info.v" + tbid + "_alarm values (" + ts + ", '2019-01-17 09:35:41.000', " + i % 10000 + ", 109.203745, 30.120334, 67.0)";
                    executeQueryJson(jsonQuery);
                    fileWriter.write(jsonQuery + ";\n");
                    try {
                        Thread.currentThread().sleep(100);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    jsonQuery = "select * from db_gps_info.v" + tbid + "_alarm limit 1000";
                    executeQueryJson(jsonQuery);
                    fileWriter.write(jsonQuery + ";\n");
                    System.out.printf("%s: loop %d\n", Thread.currentThread().getName(), i);
                }
                fileWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private class ImporterWithAlter implements Runnable {
        private int threadId;
//        private boolean schemaUpdated = false;

        public ImporterWithAlter() {

        }

        public ImporterWithAlter(int i) {
            threadId = i;
        }
        @Override
        public void run() {
            long t1 = 1547700000000L; // 2019-01-17 13:51:20.000
            Random random = new Random();
            long ts = 0L;
            int tbid = 0;
            String jsonQuery;
            String logName = "alterTestLogThread_" + threadId + ".sql";
            File file = new File("/home/" + logName);
//            File file = new File("E:\\hitsdb\\log\\" + logName);
            try {
                FileWriter fileWriter = new FileWriter(file, true);
                for (int i = 1; i <= 1000000; i++) {
//                tbid = random.nextInt(NUM_OF_TABLES) + 1;
                    tbid = random.nextInt(100) + threadId * 100 + 1;
                    ts = 30000 * random.nextInt(200) + t1;
                    if (!schemaAltered) {
//                        jsonQuery = "insert into db_gps_info.v" + tbid + "_alarm values (" + ts + ", '2019-01-17 09:35:41.000', " + i % 10000 + ", 109.203745, 30.120334, 67.0)";
                        jsonQuery = "import into db_gps_info.v" + tbid + "_alarm values (" + ts + ", '2019-01-17 09:35:41.000', " + i % 10000 + ", 109.203745, 30.120334, 67.0)";
                        executeQueryJson(jsonQuery);
                        fileWriter.write(jsonQuery + ";\n");
                    } else {
                        if (!schemaUpdated[tbid]) {
                            jsonQuery = "insert into db_gps_info.v" + tbid + "_alarm values (" + ts + ", '2019-01-17 09:35:41.000', " + i % 10000 + ", 109.203745, 30.120334, 67.0, true)";
                            executeQueryJson(jsonQuery);
                            fileWriter.write(jsonQuery + ";\n");
                            schemaUpdated[tbid] = true;
                        } else {
                            jsonQuery = "insert into db_gps_info.v" + tbid + "_alarm values (" + ts + ", '2019-01-17 09:35:41.000', " + i % 10000 + ", 109.203745, 30.120334, 67.0, true)";
                            executeQueryJson(jsonQuery);
                            fileWriter.write(jsonQuery + ";\n");
                        }
                    }

                    try {
                        Thread.currentThread().sleep(100);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    jsonQuery = "select * from db_gps_info.v" + tbid + "_alarm limit 1000";
                    executeQueryJson(jsonQuery);
                    fileWriter.write(jsonQuery + ";\n");
                    System.out.printf("%s: loop %d\n", Thread.currentThread().getName(), i);
                }
                fileWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    public void executeQueryJson(String queryJsonStr) {
        System.out.println(queryJsonStr);
        double avgTime = 0;
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(ENDPOINT);

            // Authorization
            String base64 = Base64.getEncoder().encodeToString("root:taosdata".getBytes()).toString();
            String auth = "Basic " + base64;
            httpPost.addHeader("Authorization", auth);

            StringEntity queryJsonStrEntity = new StringEntity(queryJsonStr);
            queryJsonStrEntity.setContentType(ContentType.APPLICATION_JSON.toString());
            httpPost.setEntity(queryJsonStrEntity);

            long start;
            long timeUsed;
            HttpResponse response;
            for (int i = 0; i < repeat; i++) {
                start = System.nanoTime();
                response = httpClient.execute(httpPost);
                timeUsed = System.nanoTime() - start;
                avgTime += timeUsed;
                if (200 != response.getStatusLine().getStatusCode() && 204 != response.getStatusLine().getStatusCode()) {
                    System.out.printf("Code: %d, response body: %s\n", response.getStatusLine().getStatusCode(), inputStreamToString(response.getEntity().getContent()));
                    httpClient.close();
                    return;
                }
                System.out.println(timeUsed);
            }
            avgTime = avgTime/1000000000/repeat;
            System.out.printf("Average time for each query: %f s\n", new BigDecimal(avgTime));
//            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String inputStreamToString(InputStream is) {

        String line = "";
        StringBuilder total = new StringBuilder();

        // Wrap a BufferedReader around the InputStream
        BufferedReader rd = new BufferedReader(new InputStreamReader(is));

        try {
            // Read response until the end
            while ((line = rd.readLine()) != null) {
                total.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Return full string
        return total.toString();
    }

    private String readJsonFileToString(String jsonFilePath) {
        System.out.printf("Reading query from file: %s\n", jsonFilePath);
        String queryJsonStr = "";
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(jsonFilePath));
            String lineText;
            while ((lineText = bufferedReader.readLine()) != null) {
//                System.out.println(lineText);
                queryJsonStr = queryJsonStr + lineText + "\n";
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return queryJsonStr;
    }
}
