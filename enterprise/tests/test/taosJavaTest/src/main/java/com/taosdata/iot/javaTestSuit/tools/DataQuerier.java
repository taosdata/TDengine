package com.taosdata.iot.javaTestSuit.tools;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;
import com.taosdata.jdbc.TSDBDriver;

import java.io.FileReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Jiangyi Hou
 * @since 19-1-17
 */
public class DataQuerier {

    // connection params
    private static String host = "127.0.0.1";
    private static String user = "root";
    private static String password = "taosdata";

    // db params
    private static String db;

    // query params
    private static String[] queries;
    private static int threadNum;
    private static long sleep;
    private static int repeats;

    private static int reportFreq = 10;

    public static void main(String[] args) {

        DataQuerier dataQuerier = new DataQuerier();
        dataQuerier.readParams(args[0]);
        dataQuerier.query();
    }

    private void readParams(String jsonFilePath) {
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
            queries = gson.fromJson(params.get("queries"), String[].class);
            threadNum = params.get("threadNum").getAsInt();
            sleep = params.get("sleep").getAsLong();
            repeats = params.get("repeats").getAsInt();
            reportFreq = params.get("reportFrequency").getAsInt();

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("=======================Paramerters========================");
        System.out.printf("hostIP: \t%s\n", host);
        System.out.printf("user: \t%s\n", user);
        System.out.printf("password: \t%s\n", password);
        System.out.printf("database name: \t%s\n", db);
        System.out.printf("queries: \n");
        Arrays.stream(queries).forEach(q->System.out.printf("\t%s\n", q));
        System.out.printf("number of threads: \t%d\n", threadNum);
        System.out.printf("sleep between queries (ms): \t%d\n", sleep);
        System.out.printf("report frequency (repeats): \t%d\n", reportFreq);
        System.out.printf("number of repeats: \t%d\n", repeats);
        System.out.println("==========================================================");
    }

    private class QueryTask implements Runnable {

        int threadId;
        QueryTask (int threadId) {
            this.threadId = threadId;
        }

        @Override
        public void run() {
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
            Connection connection = new ConnectionFactory().getConnection(properties);
            System.out.printf("%s thread-%d: query task started!\n",
                    new SimpleDateFormat("yy-MM-dd HH:mm:ss.SSS").format(new Timestamp(System.currentTimeMillis())),
                    threadId);
            ArrayList<String> preExecutedSqls = new ArrayList<>();
            preExecutedSqls.add("show databases");
            preExecutedSqls.add("use " + db);
            preExecutedSqls.add("show stables");
            preExecutedSqls.add("show tables");

            ArrayList<String> sqls = new ArrayList<>();

            try {
                Statement stmt = connection.createStatement();
                preExecutedSqls.forEach(sql-> {
                    try {
                        System.out.println(sql);
                        stmt.executeUpdate(sql);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.out.printf("Thread-%d: Failed to execute sql: %s\n", threadId, sql);
                        return;
                    }
                });
                System.out.printf("Thread-%d: Repeating...\n", threadId);
                int i = 1;
                while (i <= repeats) {
                    Arrays.stream(queries).forEach(sql-> {
                        try {
                            ResultSet res = stmt.executeQuery(sql);
                            ResultSetMetaData metaData = res.getMetaData();
                            while (res.next()) {
                                for (int j = 1; j <= metaData.getColumnCount(); j++) {
                                    res.getObject(j);
                                }
                            }
                            res.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                            System.out.printf("Thread-%d: Failed to execute sql: %s\n", threadId, sql);
                            return;
                        }
                    });
                    if (i % reportFreq == 0) {
                        System.out.printf("Thread-%d: has repeated the queries %d times\n", threadId, i);
                    }
                    i++;
                    Thread.currentThread().sleep(sleep);
                }
                if ( (i-1) % reportFreq != 0) {
                    System.out.printf("Thread-%d: has repeated the queries for %d times\n", threadId, (i-1));
                }
                connection.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public void query() {
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        try {
            System.out.printf("Thread-%s: Queries to be repeated:\n", "main");
//                sqls.forEach(System.out::println);
            Arrays.stream(queries).forEach(q->System.out.printf("Thread-%s: %s\n", "main", q));
            for (int i = 1; i <= threadNum; i++) {
                executorService.execute(new DataQuerier.QueryTask(i));
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {

            // wait till all threads complete their tasks

        }

        System.out.printf("%s: All thread tasks are completed!\n", new SimpleDateFormat("yy-MM-dd HH:mm:ss.SSS").format(new Timestamp(System.currentTimeMillis())));
    }
}
