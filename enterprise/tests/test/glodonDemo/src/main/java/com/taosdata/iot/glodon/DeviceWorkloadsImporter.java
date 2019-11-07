package com.taosdata.iot.glodon;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Jiangyi Hou
 * @since 19-3-1
 */
public class DeviceWorkloadsImporter {

    private static String sourceFilePath;
    private static String processedFileDir;
    private static int threadNum;
    private static int batchSize;

    private String db = "deviceWorkloads";
    private String stb = "device";
    private int deviceTypeCount = 0;
    private Map<String, String> tables = new HashMap<>();
    private Map<String, TreeSet<String>> schemasMap = new HashMap<>();
    private Map<TreeSet<String>, String> schemas = new HashMap<>();
    private Map<String, Integer> items = new HashMap<>();
    private String url = "jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata";
    private Properties props = new Properties();
    private File[] processedFiles;

    public static void main(String[] args) {

        DeviceWorkloadsImporter importer = new DeviceWorkloadsImporter();
        if (args.length < 1) {
            sourceFilePath = "/home/jyhou/glodon/deviceWorkloads.csv";
            processedFileDir = "/home/jyhou/glodon/processed/";
            threadNum = 2;
            batchSize = 200;
        } else {
            importer.config(args[0]);
        }

        importer.processFile(sourceFilePath, processedFileDir);
        importer.createSchema();
        importer.insert();

    }

    private void config(String configFilePath) {
        try {
            System.out.printf("Reading config parameters from %s ...\n", configFilePath);
            BufferedReader reader = new BufferedReader(new FileReader(new File(configFilePath)));
            this.url = "jdbc:TAOS://" + reader.readLine() + ":0/?user=root&password=taosdata";
            sourceFilePath = reader.readLine();
            processedFileDir =  reader.readLine();
            threadNum = Integer.valueOf(reader.readLine());
            batchSize =  Integer.valueOf(reader.readLine());
            reader.close();
            System.out.println("Done.");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to open config file at: %s\n", configFilePath);
        }
    }

    private Connection getTaosConnection(String url, Properties props) {
        Connection connection = null;
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            connection = DriverManager.getConnection(url, props);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    private void processFile(String filePath, String destDir) {
        File file = new File(filePath);
        FileWriter fileWriter;
        String lineTxt = "";
        String[] record;
        try {

            if (file.exists()) {
                System.out.printf("Preprocessing data file at %s ...\n", filePath);
                BufferedReader reader = new BufferedReader(new FileReader(file));

//                String lastMsgId = "";
                while ((lineTxt = reader.readLine()) != null) {
                    record = lineTxt.split(",");
                    String cimid = record[5];
                    String ts = "'" + record[0].replaceAll("z|t|Z|T", " ") + "',";
                    String msgid = "'" + record[1] + "';";
                    String item = "_" + record[2];
                    String value = "#'" + record[3] + "'";
//                    String _class = "'" + record[4] + "',";
                    if (!tables.containsKey(cimid)) {
                        tables.put(cimid, "");
                        schemasMap.put(cimid, new TreeSet<String>());
                        file = new File(destDir + File.separator + cimid);
                        file.createNewFile();
                    } else {
                        file = new File(destDir + File.separator + cimid);
                    }
                    fileWriter = new FileWriter(file, true);
//                    System.out.println(ts + msgid + item + value + _class);
                    if ("".equals(tables.get(cimid))) {
                        // first row in file
                        fileWriter.append(ts + msgid + item + value);
                        tables.put(cimid, msgid);
                        schemasMap.get(cimid).add(item);
//                        if (!items.containsKey(item)){
//                            items.put(item, value.length());
//                        }
                    } else if (msgid.equals(tables.get(cimid))) {
                        fileWriter.append("," + item + value);
                        schemasMap.get(cimid).add(item);
//                        if (!items.containsKey(item)){
//                            items.put(item, value.length());
//                        }
                    } else {
                        fileWriter.append("\n" + ts + msgid + item + value);
                        tables.put(cimid, msgid);
                        schemasMap.get(cimid).add(item);
//                        if (!items.containsKey(item)){
//                            items.put(item, value.length());
//                        }
                    }
                    fileWriter.flush();
                    fileWriter.close();

                }
                System.out.println("Done");
                reader.close();
            } else {
                throw new IOException("File not exist.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createSchema() {
        Iterator<String> it = tables.keySet().iterator();
        String cimid;
        String tbname;
        String sql = "";
        try {
            System.out.println("Creating schema...");
//            BufferedReader reader;
//            File[] processedFiles = new File(processedFileDir).listFiles();

            MessageDigest mDigest = MessageDigest.getInstance("MD5");
            Connection connection = getTaosConnection(url, props);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("create database if not exists " + db);
            stmt.executeUpdate("use " + db);
//            stmt.executeUpdate("create table " + stb + " (ts timestamp, msgid binary(40), item binary(20), value binary(20), _class binary(80)) tags (cimid binary(40)) ");

            while (it.hasNext()) {
                cimid = it.next();
                tbname = "tb_" + new BigInteger(1, mDigest.digest(cimid.getBytes())).toString(16).substring(0, 16);
                if (!schemas.containsKey(schemasMap.get(cimid))) {
                    deviceTypeCount++;
                    sql = "create table " + stb + deviceTypeCount + " (ts timestamp, msgid binary(40) ";
                    Iterator iterator = schemasMap.get(cimid).iterator();
                    while (iterator.hasNext()) {
                        sql = sql + ", " + iterator.next() + " binary(20)";
                    }
                    sql = sql + ") tags (cimid binary(40))";
                    stmt.executeUpdate(sql);
                    schemas.put(schemasMap.get(cimid), stb + deviceTypeCount);
                    sql = "create table " + tbname + " using " + stb + deviceTypeCount + " tags ('" + cimid + "')";
                } else {
                    sql = "create table " + tbname + " using " + schemas.get(schemasMap.get(cimid)) + " tags ('" + cimid + "')";
                }
                stmt.executeUpdate(sql);
            }

            stmt.close();
            connection.close();
            System.out.println("Done.");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to create schema.");
            System.out.printf("Failure at %s\n", sql);
            throw new RuntimeException(e);
        }
    }

    private void insert(){
        long timer = System.nanoTime();
        System.out.printf("Insert data using processed files at %s ...\n", processedFileDir);
        System.out.printf("Number of threads: %d\n", threadNum);
        System.out.printf("Batch size per insert: %d\n", batchSize);
        processedFiles = new File(processedFileDir).listFiles();
        if (processedFiles.length < 1) {
            throw new RuntimeException("No processed files were found in directory: " + processedFileDir);
        } else{

            ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
            try {
                for (int i = 1; i <= threadNum; i++) {
                    executorService.execute(new InsertTask(i));
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }finally {
                executorService.shutdown();
                while (!executorService.isTerminated()) {

                    // wait till all threads complete their tasks

                }
                System.out.println("All thread tasks are shutdown!");
                timer = System.nanoTime() - timer;
                System.out.printf("Total time used: %d s\n", timer/1000000000l);
            }

        }
    }

    private class InsertTask implements Runnable {

        int threadId;
        public InsertTask(int threadId) {
            this.threadId = threadId;
        }
        @Override
        public void run() {
            System.out.printf("Thread %d starts inserting... \n", threadId);
            int assignedFileNum = processedFiles.length / threadNum;
            int startPosition = (threadId - 1) * assignedFileNum;
            if (threadId == threadNum) {
                assignedFileNum += processedFiles.length%threadNum;
            }
            int endPosition = startPosition + assignedFileNum;
            BufferedReader reader;
            String lineTxt = "";
            StringBuilder sql = null;

            try {
                MessageDigest mDigest = MessageDigest.getInstance("MD5");
                Connection connection = getTaosConnection(url, props);
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("use " + db);
                for (int f = startPosition; f < endPosition; f++) {

                    // read one processed file
                    if(processedFiles[f] == null || !processedFiles[f].exists()) {
                        throw new Exception("Can not find file, index: " + f);
                    }
                    reader = new BufferedReader(new FileReader(processedFiles[f]));

                    String tbname = "tb_" + new BigInteger(1, mDigest.digest(processedFiles[f].getName().getBytes())).toString(16).substring(0, 16);
                    sql = new StringBuilder("insert into ").append(tbname).append(" values ");
                    int r = 0;// counter in batch
                    while((lineTxt = reader.readLine()) != null) {
                        String[] values = lineTxt.split(";");
                        String tsMsgid = values[0];
                        TreeSet<String> itemValuePairs = new TreeSet<String>();
                        for(String pair : values[1].split(",")) {
                            itemValuePairs.add(pair);
                        }
                        if(itemValuePairs.size() < schemasMap.get(processedFiles[f].getName()).size()) {
                            continue;
                        }
//                        Collections.sort(itemValuePairs);

                        sql.append("(").append(tsMsgid);
                        for (String pair : itemValuePairs) {
                            sql.append(",").append(pair.split("#")[1]);
                        }
                        sql.append(") ");

                        r++;
                        if (r == batchSize || sql.length() >= 64000){
                            stmt.executeUpdate(sql.toString());
                            r = 0;
                            sql.delete(12 + tbname.length() + 8, sql.length());
                            continue;
                        }
                    }
                    if (r > 0) {
                        stmt.executeUpdate(sql.toString());
                    }
                    reader.close();
                }
                System.out.printf("Thread %d completed inserting.\n", threadId);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("Failure at %s\n", sql.toString());
                throw new RuntimeException(e);
            }
        }
    }

}
