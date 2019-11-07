import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.math.BigDecimal;

public class JsonQueryExecutor {
    // Aliyun HiTSDB server instance address
    private static final String HITSDB_PUBLIC_ADDR = "ts-2zeg78wi9xk7837t3.hitsdb.rds.aliyuncs.com";
    private static final String HITSDB_VPC_ADDR = "ts-2zeg78wi9xk7837t3.hitsdb.tsdb.aliyuncs.com";
    // Aliyun HiTSDB server instance port number
    private static final int HITSDB_PUBLIC_PORT = 3242;
    private static final int HITSDB_VPC_PORT = 8242;
    // URL
    private static String URL;
    // network option: public ? vpc
    private static String network;

    private static int repeat = 1;
    private static String jsonFilePath;

    public static void main(String[] args) throws IOException {

        JsonQueryExecutor executor = new JsonQueryExecutor();

        // queries
        if (args.length < 3) {
            System.out.println("invalid args");
            return;
        }
        jsonFilePath = args[0];
        repeat = Integer.valueOf(args[1]);
        network = args[2];
        if ("vpc".equalsIgnoreCase(args[2])) {
            System.out.println("Using VPC network connection...");
            URL = "http://" + HITSDB_VPC_ADDR + ":" + HITSDB_VPC_PORT + "/api/query";
        } else {
            System.out.println("Using public network connection...");
            URL = "http://" + HITSDB_PUBLIC_ADDR + ":" + HITSDB_PUBLIC_PORT + "/api/query";
        }
        String jsonQuery = executor.readJsonFileToString(jsonFilePath);
        executor.executeQueryJson(jsonQuery);
    }

    public String readJsonFileToString(String jsonFilePath) {
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

    public void executeQueryJson(String queryJsonStr) {
        System.out.println(queryJsonStr);
        double avgTime = 0;
        try {
            FileWriter fileWriter =  new FileWriter(new File(jsonFilePath + "_res"), true);
            StringEntity queryJsonStrEntity = new StringEntity(queryJsonStr);
            queryJsonStrEntity.setContentType(ContentType.APPLICATION_JSON.toString());
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(URL);
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
                    System.out.printf("Code: %s\n", response.getEntity().toString());
                    httpClient.close();
                    return;
                }
                System.out.println(timeUsed);
                fileWriter.write(String.valueOf(timeUsed) + "\n");
            }
            avgTime = avgTime/1000000000/repeat;
            System.out.printf("Average time for each query: %f s\n", new BigDecimal(avgTime));
            fileWriter.close();
            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
